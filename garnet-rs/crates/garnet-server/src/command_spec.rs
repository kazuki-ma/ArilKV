//! Centralized command metadata used by dispatch, routing, replication, and
//! COMMAND output.

use std::sync::OnceLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CommandId {
    Get,
    Set,
    Setex,
    Setnx,
    Strlen,
    Getrange,
    Substr,
    Getbit,
    Setbit,
    Setrange,
    Bitcount,
    Bitpos,
    Bitop,
    Bitfield,
    BitfieldRo,
    Lcs,
    Sort,
    SortRo,
    Subscribe,
    Psubscribe,
    Ssubscribe,
    Unsubscribe,
    Punsubscribe,
    Sunsubscribe,
    Publish,
    Spublish,
    Pubsub,
    Geoadd,
    Geopos,
    Geodist,
    Geohash,
    Geosearch,
    Geosearchstore,
    Del,
    Rename,
    Renamenx,
    Copy,
    Incr,
    Decr,
    Incrby,
    Decrby,
    Exists,
    Type,
    Mget,
    Mset,
    Expire,
    Expireat,
    Expiretime,
    Ttl,
    Pexpire,
    Pexpireat,
    Pexpiretime,
    Pttl,
    Persist,
    Hset,
    Hget,
    Hdel,
    Hgetall,
    Hlen,
    Hmget,
    Hmset,
    Hsetnx,
    Hexists,
    Hkeys,
    Hvals,
    Hstrlen,
    Hincrby,
    Hincrbyfloat,
    Hrandfield,
    Lpush,
    Rpush,
    Lpop,
    Rpop,
    Lrange,
    Llen,
    Lindex,
    Lpos,
    Lset,
    Ltrim,
    Lpushx,
    Rpushx,
    Lrem,
    Linsert,
    Lmove,
    Rpoplpush,
    Lmpop,
    Blmpop,
    Blpop,
    Brpop,
    Blmove,
    Brpoplpush,
    Sadd,
    Srem,
    Smembers,
    Sismember,
    Scard,
    Smismember,
    Srandmember,
    Spop,
    Smove,
    Sdiff,
    Sdiffstore,
    Sinter,
    Sintercard,
    Sinterstore,
    Sunion,
    Sunionstore,
    Zadd,
    Zrem,
    Zrange,
    Zrevrange,
    Zrangebyscore,
    Zrevrangebyscore,
    Zscore,
    Zcard,
    Zcount,
    Zrank,
    Zrevrank,
    Zincrby,
    Zremrangebyrank,
    Zremrangebyscore,
    Zmscore,
    Zrandmember,
    Zpopmin,
    Zpopmax,
    Bzpopmin,
    Bzpopmax,
    Zdiff,
    Zdiffstore,
    Zinter,
    Zinterstore,
    Zlexcount,
    Zrangestore,
    Zrangebylex,
    Zrevrangebylex,
    Zremrangebylex,
    Zintercard,
    Zmpop,
    Bzmpop,
    Zunion,
    Zunionstore,
    Xadd,
    Xdel,
    Xgroup,
    Xreadgroup,
    Xread,
    Xack,
    Xpending,
    Xclaim,
    Xautoclaim,
    Xsetid,
    Xinfo,
    Xlen,
    Xrange,
    Xrevrange,
    Xtrim,
    Multi,
    Exec,
    Discard,
    Watch,
    Unwatch,
    Asking,
    Acl,
    Cluster,
    Failover,
    Monitor,
    Shutdown,
    Hello,
    Lastsave,
    Auth,
    Select,
    Client,
    Role,
    Wait,
    Waitaof,
    Save,
    Bgsave,
    Bgrewriteaof,
    Readonly,
    Readwrite,
    Reset,
    Lolwut,
    Ping,
    Echo,
    Info,
    Memory,
    Dbsize,
    Debug,
    Object,
    Keys,
    Randomkey,
    Scan,
    Hscan,
    Sscan,
    Zscan,
    Flushdb,
    Flushall,
    Function,
    Script,
    Eval,
    EvalRo,
    Evalsha,
    EvalshaRo,
    Fcall,
    FcallRo,
    Config,
    Command,
    Dump,
    Restore,
    RestoreAsking,
    Getdel,
    Getset,
    Psetex,
    Append,
    Getex,
    Incrbyfloat,
    Msetnx,
    Pfadd,
    Pfcount,
    Pfmerge,
    Pfdebug,
    Pfselftest,
    Quit,
    Time,
    Touch,
    Unlink,
    Move,
    Swapdb,
    Latency,
    Module,
    Slowlog,
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

const REPLICATION_PROTOCOL_COMMANDS: [&[u8]; 5] =
    [b"REPLICAOF", b"SLAVEOF", b"REPLCONF", b"PSYNC", b"SYNC"];
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
        id: CommandId::Setex,
        name_upper: b"SETEX",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Setnx,
        name_upper: b"SETNX",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Strlen,
        name_upper: b"STRLEN",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Getrange,
        name_upper: b"GETRANGE",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Substr,
        name_upper: b"SUBSTR",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Getbit,
        name_upper: b"GETBIT",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Setbit,
        name_upper: b"SETBIT",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Setrange,
        name_upper: b"SETRANGE",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Bitcount,
        name_upper: b"BITCOUNT",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Bitpos,
        name_upper: b"BITPOS",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Bitop,
        name_upper: b"BITOP",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Bitfield,
        name_upper: b"BITFIELD",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::BitfieldRo,
        name_upper: b"BITFIELD_RO",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Lcs,
        name_upper: b"LCS",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Sort,
        name_upper: b"SORT",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::SortRo,
        name_upper: b"SORT_RO",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Subscribe,
        name_upper: b"SUBSCRIBE",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Psubscribe,
        name_upper: b"PSUBSCRIBE",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Ssubscribe,
        name_upper: b"SSUBSCRIBE",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Unsubscribe,
        name_upper: b"UNSUBSCRIBE",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(1)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Punsubscribe,
        name_upper: b"PUNSUBSCRIBE",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(1)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Sunsubscribe,
        name_upper: b"SUNSUBSCRIBE",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(1)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Publish,
        name_upper: b"PUBLISH",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Spublish,
        name_upper: b"SPUBLISH",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Pubsub,
        name_upper: b"PUBSUB",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Geoadd,
        name_upper: b"GEOADD",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(5)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Geopos,
        name_upper: b"GEOPOS",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Geodist,
        name_upper: b"GEODIST",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Geohash,
        name_upper: b"GEOHASH",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Geosearch,
        name_upper: b"GEOSEARCH",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(7)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Geosearchstore,
        name_upper: b"GEOSEARCHSTORE",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(8)),
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
        id: CommandId::Rename,
        name_upper: b"RENAME",
        key_access_pattern: KeyAccessPattern::AllKeysFromArg1,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Renamenx,
        name_upper: b"RENAMENX",
        key_access_pattern: KeyAccessPattern::AllKeysFromArg1,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Copy,
        name_upper: b"COPY",
        key_access_pattern: KeyAccessPattern::AllKeysFromArg1,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
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
        id: CommandId::Incrby,
        name_upper: b"INCRBY",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Decrby,
        name_upper: b"DECRBY",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Exists,
        name_upper: b"EXISTS",
        key_access_pattern: KeyAccessPattern::AllKeysFromArg1,
        owner_routing_policy: OwnerRoutingPolicy::SingleKeyOnly,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Type,
        name_upper: b"TYPE",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Mget,
        name_upper: b"MGET",
        key_access_pattern: KeyAccessPattern::AllKeysFromArg1,
        owner_routing_policy: OwnerRoutingPolicy::SingleKeyOnly,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Mset,
        name_upper: b"MSET",
        key_access_pattern: KeyAccessPattern::AllKeysFromArg1,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
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
        id: CommandId::Expireat,
        name_upper: b"EXPIREAT",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Expiretime,
        name_upper: b"EXPIRETIME",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(2)),
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
        id: CommandId::Pexpireat,
        name_upper: b"PEXPIREAT",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Pexpiretime,
        name_upper: b"PEXPIRETIME",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(2)),
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
        id: CommandId::Hlen,
        name_upper: b"HLEN",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Hmget,
        name_upper: b"HMGET",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Hmset,
        name_upper: b"HMSET",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Hsetnx,
        name_upper: b"HSETNX",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Hexists,
        name_upper: b"HEXISTS",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Hkeys,
        name_upper: b"HKEYS",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Hvals,
        name_upper: b"HVALS",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Hstrlen,
        name_upper: b"HSTRLEN",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Hincrby,
        name_upper: b"HINCRBY",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Hincrbyfloat,
        name_upper: b"HINCRBYFLOAT",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Hrandfield,
        name_upper: b"HRANDFIELD",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
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
        id: CommandId::Llen,
        name_upper: b"LLEN",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Lindex,
        name_upper: b"LINDEX",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Lpos,
        name_upper: b"LPOS",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Lset,
        name_upper: b"LSET",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Ltrim,
        name_upper: b"LTRIM",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Lpushx,
        name_upper: b"LPUSHX",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Rpushx,
        name_upper: b"RPUSHX",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Lrem,
        name_upper: b"LREM",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Linsert,
        name_upper: b"LINSERT",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(5)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Lmove,
        name_upper: b"LMOVE",
        key_access_pattern: KeyAccessPattern::AllKeysFromArg1,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(5)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Rpoplpush,
        name_upper: b"RPOPLPUSH",
        key_access_pattern: KeyAccessPattern::AllKeysFromArg1,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Lmpop,
        name_upper: b"LMPOP",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Blmpop,
        name_upper: b"BLMPOP",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(5)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Blpop,
        name_upper: b"BLPOP",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Brpop,
        name_upper: b"BRPOP",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Blmove,
        name_upper: b"BLMOVE",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(6)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Brpoplpush,
        name_upper: b"BRPOPLPUSH",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(4)),
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
        id: CommandId::Scard,
        name_upper: b"SCARD",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Smismember,
        name_upper: b"SMISMEMBER",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Srandmember,
        name_upper: b"SRANDMEMBER",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Spop,
        name_upper: b"SPOP",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Smove,
        name_upper: b"SMOVE",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Sdiff,
        name_upper: b"SDIFF",
        key_access_pattern: KeyAccessPattern::AllKeysFromArg1,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Sdiffstore,
        name_upper: b"SDIFFSTORE",
        key_access_pattern: KeyAccessPattern::AllKeysFromArg1,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Sinter,
        name_upper: b"SINTER",
        key_access_pattern: KeyAccessPattern::AllKeysFromArg1,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Sintercard,
        name_upper: b"SINTERCARD",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Sinterstore,
        name_upper: b"SINTERSTORE",
        key_access_pattern: KeyAccessPattern::AllKeysFromArg1,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Sunion,
        name_upper: b"SUNION",
        key_access_pattern: KeyAccessPattern::AllKeysFromArg1,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Sunionstore,
        name_upper: b"SUNIONSTORE",
        key_access_pattern: KeyAccessPattern::AllKeysFromArg1,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
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
        id: CommandId::Zrevrange,
        name_upper: b"ZREVRANGE",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Zrangebyscore,
        name_upper: b"ZRANGEBYSCORE",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Zrevrangebyscore,
        name_upper: b"ZREVRANGEBYSCORE",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(4)),
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
        id: CommandId::Zcard,
        name_upper: b"ZCARD",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Zcount,
        name_upper: b"ZCOUNT",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Zrank,
        name_upper: b"ZRANK",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Zrevrank,
        name_upper: b"ZREVRANK",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Zincrby,
        name_upper: b"ZINCRBY",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Zremrangebyrank,
        name_upper: b"ZREMRANGEBYRANK",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Zremrangebyscore,
        name_upper: b"ZREMRANGEBYSCORE",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Zmscore,
        name_upper: b"ZMSCORE",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Zrandmember,
        name_upper: b"ZRANDMEMBER",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Zpopmin,
        name_upper: b"ZPOPMIN",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Zpopmax,
        name_upper: b"ZPOPMAX",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Bzpopmin,
        name_upper: b"BZPOPMIN",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Bzpopmax,
        name_upper: b"BZPOPMAX",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Zdiff,
        name_upper: b"ZDIFF",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Zdiffstore,
        name_upper: b"ZDIFFSTORE",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Zinter,
        name_upper: b"ZINTER",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Zinterstore,
        name_upper: b"ZINTERSTORE",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Zlexcount,
        name_upper: b"ZLEXCOUNT",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Zrangestore,
        name_upper: b"ZRANGESTORE",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(5)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Zrangebylex,
        name_upper: b"ZRANGEBYLEX",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Zrevrangebylex,
        name_upper: b"ZREVRANGEBYLEX",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Zremrangebylex,
        name_upper: b"ZREMRANGEBYLEX",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Zintercard,
        name_upper: b"ZINTERCARD",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Zmpop,
        name_upper: b"ZMPOP",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Bzmpop,
        name_upper: b"BZMPOP",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(5)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Zunion,
        name_upper: b"ZUNION",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Zunionstore,
        name_upper: b"ZUNIONSTORE",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Xadd,
        name_upper: b"XADD",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(5)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Xdel,
        name_upper: b"XDEL",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Xgroup,
        name_upper: b"XGROUP",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Xreadgroup,
        name_upper: b"XREADGROUP",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(8)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Xread,
        name_upper: b"XREAD",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Xack,
        name_upper: b"XACK",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Xpending,
        name_upper: b"XPENDING",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Xclaim,
        name_upper: b"XCLAIM",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(6)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Xautoclaim,
        name_upper: b"XAUTOCLAIM",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(6)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Xsetid,
        name_upper: b"XSETID",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Xinfo,
        name_upper: b"XINFO",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Xlen,
        name_upper: b"XLEN",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Xrange,
        name_upper: b"XRANGE",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Xrevrange,
        name_upper: b"XREVRANGE",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Xtrim,
        name_upper: b"XTRIM",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(4)),
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
        id: CommandId::Acl,
        name_upper: b"ACL",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Cluster,
        name_upper: b"CLUSTER",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Failover,
        name_upper: b"FAILOVER",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(1)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Monitor,
        name_upper: b"MONITOR",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(1)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Shutdown,
        name_upper: b"SHUTDOWN",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(1)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Hello,
        name_upper: b"HELLO",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(1)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Lastsave,
        name_upper: b"LASTSAVE",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(1)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Auth,
        name_upper: b"AUTH",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Select,
        name_upper: b"SELECT",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Client,
        name_upper: b"CLIENT",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Role,
        name_upper: b"ROLE",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(1)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Wait,
        name_upper: b"WAIT",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Waitaof,
        name_upper: b"WAITAOF",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Save,
        name_upper: b"SAVE",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(1)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Bgsave,
        name_upper: b"BGSAVE",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(1)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Bgrewriteaof,
        name_upper: b"BGREWRITEAOF",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(1)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Readonly,
        name_upper: b"READONLY",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(1)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Readwrite,
        name_upper: b"READWRITE",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(1)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Reset,
        name_upper: b"RESET",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(1)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Lolwut,
        name_upper: b"LOLWUT",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(1)),
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
        id: CommandId::Memory,
        name_upper: b"MEMORY",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
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
        id: CommandId::Debug,
        name_upper: b"DEBUG",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Object,
        name_upper: b"OBJECT",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Keys,
        name_upper: b"KEYS",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Randomkey,
        name_upper: b"RANDOMKEY",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(1)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Scan,
        name_upper: b"SCAN",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Hscan,
        name_upper: b"HSCAN",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Sscan,
        name_upper: b"SSCAN",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Zscan,
        name_upper: b"ZSCAN",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Flushdb,
        name_upper: b"FLUSHDB",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(1)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Flushall,
        name_upper: b"FLUSHALL",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(1)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Function,
        name_upper: b"FUNCTION",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Script,
        name_upper: b"SCRIPT",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Eval,
        name_upper: b"EVAL",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::EvalRo,
        name_upper: b"EVAL_RO",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Evalsha,
        name_upper: b"EVALSHA",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::EvalshaRo,
        name_upper: b"EVALSHA_RO",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Fcall,
        name_upper: b"FCALL",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::FcallRo,
        name_upper: b"FCALL_RO",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Config,
        name_upper: b"CONFIG",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
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
        id: CommandId::Dump,
        name_upper: b"DUMP",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Restore,
        name_upper: b"RESTORE",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::RestoreAsking,
        name_upper: b"RESTORE-ASKING",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Getdel,
        name_upper: b"GETDEL",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Getset,
        name_upper: b"GETSET",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Psetex,
        name_upper: b"PSETEX",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(4)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Append,
        name_upper: b"APPEND",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Getex,
        name_upper: b"GETEX",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Incrbyfloat,
        name_upper: b"INCRBYFLOAT",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Msetnx,
        name_upper: b"MSETNX",
        key_access_pattern: KeyAccessPattern::AllKeysFromArg1,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Pfadd,
        name_upper: b"PFADD",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Pfcount,
        name_upper: b"PFCOUNT",
        key_access_pattern: KeyAccessPattern::AllKeysFromArg1,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Pfmerge,
        name_upper: b"PFMERGE",
        key_access_pattern: KeyAccessPattern::AllKeysFromArg1,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Pfdebug,
        name_upper: b"PFDEBUG",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Pfselftest,
        name_upper: b"PFSELFTEST",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(1)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Quit,
        name_upper: b"QUIT",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(1)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Time,
        name_upper: b"TIME",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(1)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Touch,
        name_upper: b"TOUCH",
        key_access_pattern: KeyAccessPattern::AllKeysFromArg1,
        owner_routing_policy: OwnerRoutingPolicy::SingleKeyOnly,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Unlink,
        name_upper: b"UNLINK",
        key_access_pattern: KeyAccessPattern::AllKeysFromArg1,
        owner_routing_policy: OwnerRoutingPolicy::SingleKeyOnly,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Move,
        name_upper: b"MOVE",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Swapdb,
        name_upper: b"SWAPDB",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Exact(3)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Latency,
        name_upper: b"LATENCY",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Module,
        name_upper: b"MODULE",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Slowlog,
        name_upper: b"SLOWLOG",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: Some(ArityPolicy::Min(2)),
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
        assert!(names.contains(&&b"SLAVEOF"[..]));
        assert!(names.contains(&&b"REPLCONF"[..]));
        assert!(names.contains(&&b"PSYNC"[..]));
        assert!(names.contains(&&b"SYNC"[..]));
    }

    #[test]
    fn owner_routing_restricts_del_to_single_key() {
        assert!(command_is_owner_routable(CommandId::Del, 2));
        assert!(!command_is_owner_routable(CommandId::Del, 3));
        assert!(command_is_owner_routable(CommandId::Set, 3));
        assert!(command_is_owner_routable(CommandId::Exists, 2));
        assert!(!command_is_owner_routable(CommandId::Exists, 3));
        assert!(command_is_owner_routable(CommandId::Mget, 2));
        assert!(!command_is_owner_routable(CommandId::Mget, 3));
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
            command_key_access_pattern(CommandId::Mset),
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
        assert!(command_has_valid_arity(CommandId::Incrby, 3));
        assert!(!command_has_valid_arity(CommandId::Incrby, 2));
        assert!(command_has_valid_arity(CommandId::Rename, 3));
        assert!(!command_has_valid_arity(CommandId::Rename, 2));
        assert!(command_has_valid_arity(CommandId::Renamenx, 3));
        assert!(!command_has_valid_arity(CommandId::Renamenx, 2));
        assert!(command_has_valid_arity(CommandId::Copy, 3));
        assert!(command_has_valid_arity(CommandId::Copy, 5));
        assert!(!command_has_valid_arity(CommandId::Copy, 2));
        assert!(command_has_valid_arity(CommandId::Setex, 4));
        assert!(!command_has_valid_arity(CommandId::Setex, 3));
        assert!(command_has_valid_arity(CommandId::Setnx, 3));
        assert!(!command_has_valid_arity(CommandId::Setnx, 2));
        assert!(command_has_valid_arity(CommandId::Strlen, 2));
        assert!(!command_has_valid_arity(CommandId::Strlen, 3));
        assert!(command_has_valid_arity(CommandId::Getrange, 4));
        assert!(command_has_valid_arity(CommandId::Substr, 4));
        assert!(!command_has_valid_arity(CommandId::Substr, 3));
        assert!(command_has_valid_arity(CommandId::Getbit, 3));
        assert!(!command_has_valid_arity(CommandId::Getbit, 2));
        assert!(command_has_valid_arity(CommandId::Setbit, 4));
        assert!(!command_has_valid_arity(CommandId::Setbit, 3));
        assert!(command_has_valid_arity(CommandId::Setrange, 4));
        assert!(!command_has_valid_arity(CommandId::Setrange, 3));
        assert!(command_has_valid_arity(CommandId::Bitcount, 2));
        assert!(command_has_valid_arity(CommandId::Bitcount, 4));
        assert!(command_has_valid_arity(CommandId::Bitcount, 5));
        assert!(!command_has_valid_arity(CommandId::Bitcount, 1));
        assert!(command_has_valid_arity(CommandId::Bitpos, 3));
        assert!(command_has_valid_arity(CommandId::Bitpos, 6));
        assert!(!command_has_valid_arity(CommandId::Bitpos, 2));
        assert!(command_has_valid_arity(CommandId::Bitop, 4));
        assert!(command_has_valid_arity(CommandId::Bitop, 6));
        assert!(!command_has_valid_arity(CommandId::Bitop, 3));
        assert!(command_has_valid_arity(CommandId::Bitfield, 2));
        assert!(command_has_valid_arity(CommandId::Bitfield, 7));
        assert!(!command_has_valid_arity(CommandId::Bitfield, 1));
        assert!(command_has_valid_arity(CommandId::BitfieldRo, 2));
        assert!(command_has_valid_arity(CommandId::BitfieldRo, 5));
        assert!(!command_has_valid_arity(CommandId::BitfieldRo, 1));
        assert!(command_has_valid_arity(CommandId::Lcs, 3));
        assert!(command_has_valid_arity(CommandId::Lcs, 7));
        assert!(!command_has_valid_arity(CommandId::Lcs, 2));
        assert!(command_has_valid_arity(CommandId::Sort, 2));
        assert!(command_has_valid_arity(CommandId::Sort, 9));
        assert!(!command_has_valid_arity(CommandId::Sort, 1));
        assert!(command_has_valid_arity(CommandId::SortRo, 2));
        assert!(command_has_valid_arity(CommandId::SortRo, 9));
        assert!(!command_has_valid_arity(CommandId::SortRo, 1));
        assert!(command_has_valid_arity(CommandId::Subscribe, 2));
        assert!(command_has_valid_arity(CommandId::Subscribe, 4));
        assert!(!command_has_valid_arity(CommandId::Subscribe, 1));
        assert!(command_has_valid_arity(CommandId::Psubscribe, 2));
        assert!(command_has_valid_arity(CommandId::Psubscribe, 4));
        assert!(!command_has_valid_arity(CommandId::Psubscribe, 1));
        assert!(command_has_valid_arity(CommandId::Ssubscribe, 2));
        assert!(command_has_valid_arity(CommandId::Ssubscribe, 4));
        assert!(!command_has_valid_arity(CommandId::Ssubscribe, 1));
        assert!(command_has_valid_arity(CommandId::Unsubscribe, 1));
        assert!(command_has_valid_arity(CommandId::Unsubscribe, 3));
        assert!(command_has_valid_arity(CommandId::Punsubscribe, 1));
        assert!(command_has_valid_arity(CommandId::Sunsubscribe, 1));
        assert!(command_has_valid_arity(CommandId::Publish, 3));
        assert!(!command_has_valid_arity(CommandId::Publish, 2));
        assert!(command_has_valid_arity(CommandId::Spublish, 3));
        assert!(!command_has_valid_arity(CommandId::Spublish, 2));
        assert!(command_has_valid_arity(CommandId::Pubsub, 2));
        assert!(command_has_valid_arity(CommandId::Pubsub, 4));
        assert!(!command_has_valid_arity(CommandId::Pubsub, 1));
        assert!(command_has_valid_arity(CommandId::Geoadd, 5));
        assert!(command_has_valid_arity(CommandId::Geoadd, 9));
        assert!(!command_has_valid_arity(CommandId::Geoadd, 4));
        assert!(command_has_valid_arity(CommandId::Geopos, 3));
        assert!(command_has_valid_arity(CommandId::Geopos, 5));
        assert!(!command_has_valid_arity(CommandId::Geopos, 2));
        assert!(command_has_valid_arity(CommandId::Geodist, 4));
        assert!(command_has_valid_arity(CommandId::Geodist, 5));
        assert!(!command_has_valid_arity(CommandId::Geodist, 3));
        assert!(command_has_valid_arity(CommandId::Geohash, 3));
        assert!(command_has_valid_arity(CommandId::Geohash, 6));
        assert!(!command_has_valid_arity(CommandId::Geohash, 2));
        assert!(command_has_valid_arity(CommandId::Geosearch, 7));
        assert!(command_has_valid_arity(CommandId::Geosearch, 11));
        assert!(!command_has_valid_arity(CommandId::Geosearch, 6));
        assert!(command_has_valid_arity(CommandId::Geosearchstore, 8));
        assert!(command_has_valid_arity(CommandId::Geosearchstore, 12));
        assert!(!command_has_valid_arity(CommandId::Geosearchstore, 7));
        assert!(command_has_valid_arity(CommandId::Exists, 2));
        assert!(command_has_valid_arity(CommandId::Exists, 3));
        assert!(!command_has_valid_arity(CommandId::Type, 3));
        assert!(command_has_valid_arity(CommandId::Type, 2));
        assert!(command_has_valid_arity(CommandId::Expiretime, 2));
        assert!(!command_has_valid_arity(CommandId::Expiretime, 3));
        assert!(command_has_valid_arity(CommandId::Pexpiretime, 2));
        assert!(!command_has_valid_arity(CommandId::Pexpiretime, 3));
        assert!(!command_has_valid_arity(CommandId::Mset, 2));
        assert!(command_has_valid_arity(CommandId::Mset, 3));
        assert!(command_has_valid_arity(CommandId::Hlen, 2));
        assert!(!command_has_valid_arity(CommandId::Hlen, 3));
        assert!(!command_has_valid_arity(CommandId::Hmset, 3));
        assert!(command_has_valid_arity(CommandId::Hmset, 4));
        assert!(command_has_valid_arity(CommandId::Hmset, 6));
        assert!(!command_has_valid_arity(CommandId::Hmget, 2));
        assert!(command_has_valid_arity(CommandId::Hmget, 3));
        assert!(!command_has_valid_arity(CommandId::Hrandfield, 1));
        assert!(command_has_valid_arity(CommandId::Hrandfield, 2));
        assert!(command_has_valid_arity(CommandId::Hrandfield, 4));
        assert!(!command_has_valid_arity(CommandId::Hello, 0));
        assert!(command_has_valid_arity(CommandId::Hello, 1));
        assert!(command_has_valid_arity(CommandId::Hello, 2));
        assert!(command_has_valid_arity(CommandId::Lastsave, 1));
        assert!(!command_has_valid_arity(CommandId::Lastsave, 2));
        assert!(command_has_valid_arity(CommandId::Auth, 2));
        assert!(command_has_valid_arity(CommandId::Auth, 3));
        assert!(!command_has_valid_arity(CommandId::Auth, 1));
        assert!(command_has_valid_arity(CommandId::Select, 2));
        assert!(!command_has_valid_arity(CommandId::Select, 1));
        assert!(command_has_valid_arity(CommandId::Client, 2));
        assert!(command_has_valid_arity(CommandId::Client, 4));
        assert!(!command_has_valid_arity(CommandId::Client, 1));
        assert!(command_has_valid_arity(CommandId::Role, 1));
        assert!(!command_has_valid_arity(CommandId::Role, 2));
        assert!(command_has_valid_arity(CommandId::Wait, 3));
        assert!(!command_has_valid_arity(CommandId::Wait, 2));
        assert!(command_has_valid_arity(CommandId::Waitaof, 4));
        assert!(!command_has_valid_arity(CommandId::Waitaof, 3));
        assert!(command_has_valid_arity(CommandId::Save, 1));
        assert!(!command_has_valid_arity(CommandId::Save, 2));
        assert!(command_has_valid_arity(CommandId::Bgsave, 1));
        assert!(command_has_valid_arity(CommandId::Bgsave, 2));
        assert!(!command_has_valid_arity(CommandId::Bgsave, 0));
        assert!(command_has_valid_arity(CommandId::Bgrewriteaof, 1));
        assert!(!command_has_valid_arity(CommandId::Bgrewriteaof, 2));
        assert!(command_has_valid_arity(CommandId::Readonly, 1));
        assert!(!command_has_valid_arity(CommandId::Readonly, 2));
        assert!(command_has_valid_arity(CommandId::Readwrite, 1));
        assert!(!command_has_valid_arity(CommandId::Readwrite, 2));
        assert!(command_has_valid_arity(CommandId::Reset, 1));
        assert!(!command_has_valid_arity(CommandId::Reset, 2));
        assert!(command_has_valid_arity(CommandId::Lolwut, 1));
        assert!(command_has_valid_arity(CommandId::Lolwut, 3));
        assert!(!command_has_valid_arity(CommandId::Lolwut, 0));
        assert!(command_has_valid_arity(CommandId::Acl, 2));
        assert!(command_has_valid_arity(CommandId::Acl, 5));
        assert!(!command_has_valid_arity(CommandId::Acl, 1));
        assert!(command_has_valid_arity(CommandId::Cluster, 2));
        assert!(command_has_valid_arity(CommandId::Cluster, 4));
        assert!(!command_has_valid_arity(CommandId::Cluster, 1));
        assert!(command_has_valid_arity(CommandId::Failover, 1));
        assert!(command_has_valid_arity(CommandId::Failover, 4));
        assert!(!command_has_valid_arity(CommandId::Failover, 0));
        assert!(command_has_valid_arity(CommandId::Monitor, 1));
        assert!(!command_has_valid_arity(CommandId::Monitor, 2));
        assert!(command_has_valid_arity(CommandId::Shutdown, 1));
        assert!(command_has_valid_arity(CommandId::Shutdown, 3));
        assert!(!command_has_valid_arity(CommandId::Shutdown, 0));
        assert!(!command_has_valid_arity(CommandId::Memory, 1));
        assert!(command_has_valid_arity(CommandId::Memory, 2));
        assert!(command_has_valid_arity(CommandId::Memory, 3));
        assert!(command_has_valid_arity(CommandId::Flushdb, 1));
        assert!(!command_has_valid_arity(CommandId::Flushdb, 2));
        assert!(command_has_valid_arity(CommandId::Flushall, 1));
        assert!(command_has_valid_arity(CommandId::Function, 2));
        assert!(!command_has_valid_arity(CommandId::Function, 1));
        assert!(command_has_valid_arity(CommandId::Script, 2));
        assert!(command_has_valid_arity(CommandId::Script, 3));
        assert!(!command_has_valid_arity(CommandId::Script, 1));
        assert!(command_has_valid_arity(CommandId::Eval, 3));
        assert!(command_has_valid_arity(CommandId::Eval, 6));
        assert!(!command_has_valid_arity(CommandId::Eval, 2));
        assert!(command_has_valid_arity(CommandId::EvalRo, 3));
        assert!(command_has_valid_arity(CommandId::EvalRo, 6));
        assert!(!command_has_valid_arity(CommandId::EvalRo, 2));
        assert!(command_has_valid_arity(CommandId::Evalsha, 3));
        assert!(command_has_valid_arity(CommandId::Evalsha, 6));
        assert!(!command_has_valid_arity(CommandId::Evalsha, 2));
        assert!(command_has_valid_arity(CommandId::EvalshaRo, 3));
        assert!(command_has_valid_arity(CommandId::EvalshaRo, 6));
        assert!(!command_has_valid_arity(CommandId::EvalshaRo, 2));
        assert!(command_has_valid_arity(CommandId::Fcall, 3));
        assert!(command_has_valid_arity(CommandId::Fcall, 6));
        assert!(!command_has_valid_arity(CommandId::Fcall, 2));
        assert!(command_has_valid_arity(CommandId::FcallRo, 3));
        assert!(command_has_valid_arity(CommandId::FcallRo, 6));
        assert!(!command_has_valid_arity(CommandId::FcallRo, 2));
        assert!(command_has_valid_arity(CommandId::Config, 2));
        assert!(command_has_valid_arity(CommandId::Config, 3));
        assert!(!command_has_valid_arity(CommandId::Config, 1));
        assert!(command_has_valid_arity(CommandId::Keys, 2));
        assert!(!command_has_valid_arity(CommandId::Keys, 1));
        assert!(!command_has_valid_arity(CommandId::Keys, 3));
        assert!(command_has_valid_arity(CommandId::Randomkey, 1));
        assert!(!command_has_valid_arity(CommandId::Randomkey, 2));
        assert!(command_has_valid_arity(CommandId::Scan, 2));
        assert!(command_has_valid_arity(CommandId::Scan, 6));
        assert!(!command_has_valid_arity(CommandId::Scan, 1));
        assert!(command_has_valid_arity(CommandId::Hscan, 3));
        assert!(command_has_valid_arity(CommandId::Hscan, 7));
        assert!(!command_has_valid_arity(CommandId::Hscan, 2));
        assert!(command_has_valid_arity(CommandId::Sscan, 3));
        assert!(command_has_valid_arity(CommandId::Sscan, 7));
        assert!(!command_has_valid_arity(CommandId::Sscan, 2));
        assert!(command_has_valid_arity(CommandId::Zscan, 3));
        assert!(command_has_valid_arity(CommandId::Zscan, 7));
        assert!(!command_has_valid_arity(CommandId::Zscan, 2));
        assert!(command_has_valid_arity(CommandId::Debug, 2));
        assert!(command_has_valid_arity(CommandId::Debug, 3));
        assert!(!command_has_valid_arity(CommandId::Debug, 1));
        assert!(command_has_valid_arity(CommandId::Set, 2));
        assert!(command_has_valid_arity(CommandId::Dump, 2));
        assert!(!command_has_valid_arity(CommandId::Dump, 3));
        assert!(command_has_valid_arity(CommandId::Restore, 4));
        assert!(command_has_valid_arity(CommandId::Restore, 8));
        assert!(!command_has_valid_arity(CommandId::Restore, 3));
        assert!(command_has_valid_arity(CommandId::RestoreAsking, 4));
        assert!(command_has_valid_arity(CommandId::RestoreAsking, 8));
        assert!(!command_has_valid_arity(CommandId::RestoreAsking, 3));
        assert!(command_has_valid_arity(CommandId::Getdel, 2));
        assert!(!command_has_valid_arity(CommandId::Getdel, 3));
        assert!(command_has_valid_arity(CommandId::Getset, 3));
        assert!(!command_has_valid_arity(CommandId::Getset, 2));
        assert!(command_has_valid_arity(CommandId::Psetex, 4));
        assert!(!command_has_valid_arity(CommandId::Psetex, 3));
        assert!(command_has_valid_arity(CommandId::Append, 3));
        assert!(!command_has_valid_arity(CommandId::Append, 2));
        assert!(command_has_valid_arity(CommandId::Getex, 2));
        assert!(command_has_valid_arity(CommandId::Getex, 4));
        assert!(!command_has_valid_arity(CommandId::Getex, 1));
        assert!(command_has_valid_arity(CommandId::Incrbyfloat, 3));
        assert!(!command_has_valid_arity(CommandId::Incrbyfloat, 2));
        assert!(command_has_valid_arity(CommandId::Msetnx, 3));
        assert!(command_has_valid_arity(CommandId::Msetnx, 5));
        assert!(!command_has_valid_arity(CommandId::Msetnx, 2));
        assert!(command_has_valid_arity(CommandId::Pfadd, 3));
        assert!(command_has_valid_arity(CommandId::Pfadd, 6));
        assert!(!command_has_valid_arity(CommandId::Pfadd, 2));
        assert!(command_has_valid_arity(CommandId::Pfcount, 2));
        assert!(command_has_valid_arity(CommandId::Pfcount, 4));
        assert!(!command_has_valid_arity(CommandId::Pfcount, 1));
        assert!(command_has_valid_arity(CommandId::Pfmerge, 3));
        assert!(command_has_valid_arity(CommandId::Pfmerge, 6));
        assert!(!command_has_valid_arity(CommandId::Pfmerge, 2));
        assert!(command_has_valid_arity(CommandId::Pfdebug, 2));
        assert!(command_has_valid_arity(CommandId::Pfdebug, 4));
        assert!(!command_has_valid_arity(CommandId::Pfdebug, 1));
        assert!(command_has_valid_arity(CommandId::Pfselftest, 1));
        assert!(!command_has_valid_arity(CommandId::Pfselftest, 2));
        assert!(command_has_valid_arity(CommandId::Llen, 2));
        assert!(!command_has_valid_arity(CommandId::Llen, 3));
        assert!(command_has_valid_arity(CommandId::Lindex, 3));
        assert!(!command_has_valid_arity(CommandId::Lindex, 2));
        assert!(command_has_valid_arity(CommandId::Lpos, 3));
        assert!(command_has_valid_arity(CommandId::Lpos, 8));
        assert!(!command_has_valid_arity(CommandId::Lpos, 2));
        assert!(command_has_valid_arity(CommandId::Lset, 4));
        assert!(!command_has_valid_arity(CommandId::Lset, 3));
        assert!(command_has_valid_arity(CommandId::Ltrim, 4));
        assert!(!command_has_valid_arity(CommandId::Ltrim, 3));
        assert!(command_has_valid_arity(CommandId::Lpushx, 3));
        assert!(command_has_valid_arity(CommandId::Lpushx, 4));
        assert!(!command_has_valid_arity(CommandId::Lpushx, 2));
        assert!(command_has_valid_arity(CommandId::Rpushx, 3));
        assert!(command_has_valid_arity(CommandId::Rpushx, 4));
        assert!(!command_has_valid_arity(CommandId::Rpushx, 2));
        assert!(command_has_valid_arity(CommandId::Lrem, 4));
        assert!(!command_has_valid_arity(CommandId::Lrem, 3));
        assert!(command_has_valid_arity(CommandId::Linsert, 5));
        assert!(!command_has_valid_arity(CommandId::Linsert, 4));
        assert!(command_has_valid_arity(CommandId::Lmove, 5));
        assert!(!command_has_valid_arity(CommandId::Lmove, 4));
        assert!(command_has_valid_arity(CommandId::Rpoplpush, 3));
        assert!(!command_has_valid_arity(CommandId::Rpoplpush, 2));
        assert!(command_has_valid_arity(CommandId::Lmpop, 4));
        assert!(command_has_valid_arity(CommandId::Lmpop, 7));
        assert!(!command_has_valid_arity(CommandId::Lmpop, 3));
        assert!(command_has_valid_arity(CommandId::Blmpop, 5));
        assert!(command_has_valid_arity(CommandId::Blmpop, 8));
        assert!(!command_has_valid_arity(CommandId::Blmpop, 4));
        assert!(command_has_valid_arity(CommandId::Blpop, 3));
        assert!(command_has_valid_arity(CommandId::Blpop, 5));
        assert!(!command_has_valid_arity(CommandId::Blpop, 2));
        assert!(command_has_valid_arity(CommandId::Brpop, 3));
        assert!(command_has_valid_arity(CommandId::Brpop, 5));
        assert!(!command_has_valid_arity(CommandId::Brpop, 2));
        assert!(command_has_valid_arity(CommandId::Blmove, 6));
        assert!(!command_has_valid_arity(CommandId::Blmove, 5));
        assert!(command_has_valid_arity(CommandId::Brpoplpush, 4));
        assert!(!command_has_valid_arity(CommandId::Brpoplpush, 3));
        assert!(command_has_valid_arity(CommandId::Scard, 2));
        assert!(!command_has_valid_arity(CommandId::Scard, 3));
        assert!(command_has_valid_arity(CommandId::Smismember, 3));
        assert!(command_has_valid_arity(CommandId::Smismember, 4));
        assert!(!command_has_valid_arity(CommandId::Smismember, 2));
        assert!(command_has_valid_arity(CommandId::Srandmember, 2));
        assert!(command_has_valid_arity(CommandId::Srandmember, 3));
        assert!(!command_has_valid_arity(CommandId::Srandmember, 1));
        assert!(command_has_valid_arity(CommandId::Spop, 2));
        assert!(command_has_valid_arity(CommandId::Spop, 3));
        assert!(!command_has_valid_arity(CommandId::Spop, 1));
        assert!(command_has_valid_arity(CommandId::Smove, 4));
        assert!(!command_has_valid_arity(CommandId::Smove, 3));
        assert!(command_has_valid_arity(CommandId::Sdiff, 2));
        assert!(command_has_valid_arity(CommandId::Sdiff, 4));
        assert!(!command_has_valid_arity(CommandId::Sdiff, 1));
        assert!(command_has_valid_arity(CommandId::Sdiffstore, 3));
        assert!(command_has_valid_arity(CommandId::Sdiffstore, 5));
        assert!(!command_has_valid_arity(CommandId::Sdiffstore, 2));
        assert!(command_has_valid_arity(CommandId::Sinter, 2));
        assert!(command_has_valid_arity(CommandId::Sinter, 4));
        assert!(!command_has_valid_arity(CommandId::Sinter, 1));
        assert!(command_has_valid_arity(CommandId::Sintercard, 3));
        assert!(command_has_valid_arity(CommandId::Sintercard, 6));
        assert!(!command_has_valid_arity(CommandId::Sintercard, 2));
        assert!(command_has_valid_arity(CommandId::Sinterstore, 3));
        assert!(command_has_valid_arity(CommandId::Sinterstore, 5));
        assert!(!command_has_valid_arity(CommandId::Sinterstore, 2));
        assert!(command_has_valid_arity(CommandId::Sunion, 2));
        assert!(command_has_valid_arity(CommandId::Sunion, 4));
        assert!(!command_has_valid_arity(CommandId::Sunion, 1));
        assert!(command_has_valid_arity(CommandId::Sunionstore, 3));
        assert!(command_has_valid_arity(CommandId::Sunionstore, 5));
        assert!(!command_has_valid_arity(CommandId::Sunionstore, 2));
        assert!(command_has_valid_arity(CommandId::Zcard, 2));
        assert!(!command_has_valid_arity(CommandId::Zcard, 3));
        assert!(command_has_valid_arity(CommandId::Zcount, 4));
        assert!(!command_has_valid_arity(CommandId::Zcount, 3));
        assert!(command_has_valid_arity(CommandId::Zrevrange, 4));
        assert!(command_has_valid_arity(CommandId::Zrevrange, 5));
        assert!(!command_has_valid_arity(CommandId::Zrevrange, 3));
        assert!(command_has_valid_arity(CommandId::Zrangebyscore, 4));
        assert!(command_has_valid_arity(CommandId::Zrangebyscore, 7));
        assert!(!command_has_valid_arity(CommandId::Zrangebyscore, 3));
        assert!(command_has_valid_arity(CommandId::Zrevrangebyscore, 4));
        assert!(command_has_valid_arity(CommandId::Zrevrangebyscore, 7));
        assert!(!command_has_valid_arity(CommandId::Zrevrangebyscore, 3));
        assert!(command_has_valid_arity(CommandId::Zrank, 3));
        assert!(!command_has_valid_arity(CommandId::Zrank, 2));
        assert!(command_has_valid_arity(CommandId::Zrevrank, 3));
        assert!(!command_has_valid_arity(CommandId::Zrevrank, 2));
        assert!(command_has_valid_arity(CommandId::Zincrby, 4));
        assert!(!command_has_valid_arity(CommandId::Zincrby, 3));
        assert!(command_has_valid_arity(CommandId::Zremrangebyrank, 4));
        assert!(!command_has_valid_arity(CommandId::Zremrangebyrank, 3));
        assert!(command_has_valid_arity(CommandId::Zremrangebyscore, 4));
        assert!(!command_has_valid_arity(CommandId::Zremrangebyscore, 3));
        assert!(command_has_valid_arity(CommandId::Zmscore, 3));
        assert!(command_has_valid_arity(CommandId::Zmscore, 5));
        assert!(!command_has_valid_arity(CommandId::Zmscore, 2));
        assert!(command_has_valid_arity(CommandId::Zrandmember, 2));
        assert!(command_has_valid_arity(CommandId::Zrandmember, 4));
        assert!(!command_has_valid_arity(CommandId::Zrandmember, 1));
        assert!(command_has_valid_arity(CommandId::Zpopmin, 2));
        assert!(command_has_valid_arity(CommandId::Zpopmin, 3));
        assert!(!command_has_valid_arity(CommandId::Zpopmin, 1));
        assert!(command_has_valid_arity(CommandId::Zpopmax, 2));
        assert!(command_has_valid_arity(CommandId::Zpopmax, 3));
        assert!(!command_has_valid_arity(CommandId::Zpopmax, 1));
        assert!(command_has_valid_arity(CommandId::Bzpopmin, 3));
        assert!(command_has_valid_arity(CommandId::Bzpopmin, 5));
        assert!(!command_has_valid_arity(CommandId::Bzpopmin, 2));
        assert!(command_has_valid_arity(CommandId::Bzpopmax, 3));
        assert!(command_has_valid_arity(CommandId::Bzpopmax, 5));
        assert!(!command_has_valid_arity(CommandId::Bzpopmax, 2));
        assert!(command_has_valid_arity(CommandId::Zdiff, 3));
        assert!(command_has_valid_arity(CommandId::Zdiff, 7));
        assert!(!command_has_valid_arity(CommandId::Zdiff, 2));
        assert!(command_has_valid_arity(CommandId::Zdiffstore, 4));
        assert!(command_has_valid_arity(CommandId::Zdiffstore, 7));
        assert!(!command_has_valid_arity(CommandId::Zdiffstore, 3));
        assert!(command_has_valid_arity(CommandId::Zinter, 3));
        assert!(command_has_valid_arity(CommandId::Zinter, 8));
        assert!(!command_has_valid_arity(CommandId::Zinter, 2));
        assert!(command_has_valid_arity(CommandId::Zinterstore, 4));
        assert!(command_has_valid_arity(CommandId::Zinterstore, 9));
        assert!(!command_has_valid_arity(CommandId::Zinterstore, 3));
        assert!(command_has_valid_arity(CommandId::Zlexcount, 4));
        assert!(!command_has_valid_arity(CommandId::Zlexcount, 3));
        assert!(command_has_valid_arity(CommandId::Zrangestore, 5));
        assert!(command_has_valid_arity(CommandId::Zrangestore, 9));
        assert!(!command_has_valid_arity(CommandId::Zrangestore, 4));
        assert!(command_has_valid_arity(CommandId::Zrangebylex, 4));
        assert!(command_has_valid_arity(CommandId::Zrangebylex, 7));
        assert!(!command_has_valid_arity(CommandId::Zrangebylex, 3));
        assert!(command_has_valid_arity(CommandId::Zrevrangebylex, 4));
        assert!(command_has_valid_arity(CommandId::Zrevrangebylex, 7));
        assert!(!command_has_valid_arity(CommandId::Zrevrangebylex, 3));
        assert!(command_has_valid_arity(CommandId::Zremrangebylex, 4));
        assert!(!command_has_valid_arity(CommandId::Zremrangebylex, 3));
        assert!(command_has_valid_arity(CommandId::Zintercard, 3));
        assert!(command_has_valid_arity(CommandId::Zintercard, 6));
        assert!(!command_has_valid_arity(CommandId::Zintercard, 2));
        assert!(command_has_valid_arity(CommandId::Zmpop, 4));
        assert!(command_has_valid_arity(CommandId::Zmpop, 8));
        assert!(!command_has_valid_arity(CommandId::Zmpop, 3));
        assert!(command_has_valid_arity(CommandId::Bzmpop, 5));
        assert!(command_has_valid_arity(CommandId::Bzmpop, 9));
        assert!(!command_has_valid_arity(CommandId::Bzmpop, 4));
        assert!(command_has_valid_arity(CommandId::Zunion, 3));
        assert!(command_has_valid_arity(CommandId::Zunion, 8));
        assert!(!command_has_valid_arity(CommandId::Zunion, 2));
        assert!(command_has_valid_arity(CommandId::Zunionstore, 4));
        assert!(command_has_valid_arity(CommandId::Zunionstore, 9));
        assert!(!command_has_valid_arity(CommandId::Zunionstore, 3));
        assert!(command_has_valid_arity(CommandId::Xlen, 2));
        assert!(!command_has_valid_arity(CommandId::Xlen, 3));
        assert!(command_has_valid_arity(CommandId::Xrange, 4));
        assert!(command_has_valid_arity(CommandId::Xrange, 6));
        assert!(!command_has_valid_arity(CommandId::Xrange, 3));
        assert!(command_has_valid_arity(CommandId::Xrevrange, 4));
        assert!(command_has_valid_arity(CommandId::Xrevrange, 6));
        assert!(!command_has_valid_arity(CommandId::Xrevrange, 3));
        assert!(command_has_valid_arity(CommandId::Xtrim, 4));
        assert!(command_has_valid_arity(CommandId::Xtrim, 8));
        assert!(!command_has_valid_arity(CommandId::Xtrim, 3));
        assert!(command_has_valid_arity(CommandId::Xread, 4));
        assert!(command_has_valid_arity(CommandId::Xread, 8));
        assert!(!command_has_valid_arity(CommandId::Xread, 3));
        assert!(command_has_valid_arity(CommandId::Xack, 4));
        assert!(command_has_valid_arity(CommandId::Xack, 6));
        assert!(!command_has_valid_arity(CommandId::Xack, 3));
        assert!(command_has_valid_arity(CommandId::Xpending, 3));
        assert!(command_has_valid_arity(CommandId::Xpending, 7));
        assert!(!command_has_valid_arity(CommandId::Xpending, 2));
        assert!(command_has_valid_arity(CommandId::Xclaim, 6));
        assert!(command_has_valid_arity(CommandId::Xclaim, 10));
        assert!(!command_has_valid_arity(CommandId::Xclaim, 5));
        assert!(command_has_valid_arity(CommandId::Xautoclaim, 6));
        assert!(command_has_valid_arity(CommandId::Xautoclaim, 8));
        assert!(!command_has_valid_arity(CommandId::Xautoclaim, 5));
        assert!(command_has_valid_arity(CommandId::Xsetid, 3));
        assert!(command_has_valid_arity(CommandId::Xsetid, 8));
        assert!(!command_has_valid_arity(CommandId::Xsetid, 2));
        assert!(command_has_valid_arity(CommandId::Quit, 1));
        assert!(!command_has_valid_arity(CommandId::Quit, 2));
        assert!(command_has_valid_arity(CommandId::Time, 1));
        assert!(!command_has_valid_arity(CommandId::Time, 2));
        assert!(command_has_valid_arity(CommandId::Touch, 2));
        assert!(command_has_valid_arity(CommandId::Touch, 3));
        assert!(!command_has_valid_arity(CommandId::Touch, 1));
        assert!(command_has_valid_arity(CommandId::Unlink, 2));
        assert!(command_has_valid_arity(CommandId::Unlink, 4));
        assert!(!command_has_valid_arity(CommandId::Unlink, 1));
        assert!(command_has_valid_arity(CommandId::Move, 3));
        assert!(!command_has_valid_arity(CommandId::Move, 2));
        assert!(command_has_valid_arity(CommandId::Swapdb, 3));
        assert!(!command_has_valid_arity(CommandId::Swapdb, 2));
        assert!(command_has_valid_arity(CommandId::Latency, 2));
        assert!(command_has_valid_arity(CommandId::Latency, 4));
        assert!(!command_has_valid_arity(CommandId::Latency, 1));
        assert!(command_has_valid_arity(CommandId::Module, 2));
        assert!(command_has_valid_arity(CommandId::Module, 3));
        assert!(!command_has_valid_arity(CommandId::Module, 1));
        assert!(command_has_valid_arity(CommandId::Slowlog, 2));
        assert!(command_has_valid_arity(CommandId::Slowlog, 3));
        assert!(!command_has_valid_arity(CommandId::Slowlog, 1));
    }

    #[test]
    fn command_name_upper_maps_known_and_unknown_commands() {
        assert_eq!(command_name_upper(CommandId::Set), b"SET");
        assert_eq!(command_name_upper(CommandId::Setnx), b"SETNX");
        assert_eq!(command_name_upper(CommandId::Strlen), b"STRLEN");
        assert_eq!(command_name_upper(CommandId::Getrange), b"GETRANGE");
        assert_eq!(command_name_upper(CommandId::Substr), b"SUBSTR");
        assert_eq!(command_name_upper(CommandId::Getbit), b"GETBIT");
        assert_eq!(command_name_upper(CommandId::Setbit), b"SETBIT");
        assert_eq!(command_name_upper(CommandId::Setrange), b"SETRANGE");
        assert_eq!(command_name_upper(CommandId::Bitcount), b"BITCOUNT");
        assert_eq!(command_name_upper(CommandId::Bitpos), b"BITPOS");
        assert_eq!(command_name_upper(CommandId::Bitop), b"BITOP");
        assert_eq!(command_name_upper(CommandId::Bitfield), b"BITFIELD");
        assert_eq!(command_name_upper(CommandId::BitfieldRo), b"BITFIELD_RO");
        assert_eq!(command_name_upper(CommandId::Lcs), b"LCS");
        assert_eq!(command_name_upper(CommandId::Sort), b"SORT");
        assert_eq!(command_name_upper(CommandId::SortRo), b"SORT_RO");
        assert_eq!(command_name_upper(CommandId::Subscribe), b"SUBSCRIBE");
        assert_eq!(command_name_upper(CommandId::Psubscribe), b"PSUBSCRIBE");
        assert_eq!(command_name_upper(CommandId::Ssubscribe), b"SSUBSCRIBE");
        assert_eq!(command_name_upper(CommandId::Unsubscribe), b"UNSUBSCRIBE");
        assert_eq!(command_name_upper(CommandId::Punsubscribe), b"PUNSUBSCRIBE");
        assert_eq!(command_name_upper(CommandId::Sunsubscribe), b"SUNSUBSCRIBE");
        assert_eq!(command_name_upper(CommandId::Publish), b"PUBLISH");
        assert_eq!(command_name_upper(CommandId::Spublish), b"SPUBLISH");
        assert_eq!(command_name_upper(CommandId::Pubsub), b"PUBSUB");
        assert_eq!(command_name_upper(CommandId::Geoadd), b"GEOADD");
        assert_eq!(command_name_upper(CommandId::Geopos), b"GEOPOS");
        assert_eq!(command_name_upper(CommandId::Geodist), b"GEODIST");
        assert_eq!(command_name_upper(CommandId::Geohash), b"GEOHASH");
        assert_eq!(command_name_upper(CommandId::Geosearch), b"GEOSEARCH");
        assert_eq!(
            command_name_upper(CommandId::Geosearchstore),
            b"GEOSEARCHSTORE"
        );
        assert_eq!(command_name_upper(CommandId::Lastsave), b"LASTSAVE");
        assert_eq!(command_name_upper(CommandId::Auth), b"AUTH");
        assert_eq!(command_name_upper(CommandId::Select), b"SELECT");
        assert_eq!(command_name_upper(CommandId::Client), b"CLIENT");
        assert_eq!(command_name_upper(CommandId::Role), b"ROLE");
        assert_eq!(command_name_upper(CommandId::Wait), b"WAIT");
        assert_eq!(command_name_upper(CommandId::Waitaof), b"WAITAOF");
        assert_eq!(command_name_upper(CommandId::Save), b"SAVE");
        assert_eq!(command_name_upper(CommandId::Bgsave), b"BGSAVE");
        assert_eq!(command_name_upper(CommandId::Bgrewriteaof), b"BGREWRITEAOF");
        assert_eq!(command_name_upper(CommandId::Readonly), b"READONLY");
        assert_eq!(command_name_upper(CommandId::Readwrite), b"READWRITE");
        assert_eq!(command_name_upper(CommandId::Reset), b"RESET");
        assert_eq!(command_name_upper(CommandId::Lolwut), b"LOLWUT");
        assert_eq!(command_name_upper(CommandId::Acl), b"ACL");
        assert_eq!(command_name_upper(CommandId::Cluster), b"CLUSTER");
        assert_eq!(command_name_upper(CommandId::Failover), b"FAILOVER");
        assert_eq!(command_name_upper(CommandId::Monitor), b"MONITOR");
        assert_eq!(command_name_upper(CommandId::Shutdown), b"SHUTDOWN");
        assert_eq!(command_name_upper(CommandId::Function), b"FUNCTION");
        assert_eq!(command_name_upper(CommandId::Script), b"SCRIPT");
        assert_eq!(command_name_upper(CommandId::Eval), b"EVAL");
        assert_eq!(command_name_upper(CommandId::EvalRo), b"EVAL_RO");
        assert_eq!(command_name_upper(CommandId::Evalsha), b"EVALSHA");
        assert_eq!(command_name_upper(CommandId::EvalshaRo), b"EVALSHA_RO");
        assert_eq!(command_name_upper(CommandId::Fcall), b"FCALL");
        assert_eq!(command_name_upper(CommandId::FcallRo), b"FCALL_RO");
        assert_eq!(command_name_upper(CommandId::Config), b"CONFIG");
        assert_eq!(command_name_upper(CommandId::Command), b"COMMAND");
        assert_eq!(command_name_upper(CommandId::Expireat), b"EXPIREAT");
        assert_eq!(command_name_upper(CommandId::Pexpiretime), b"PEXPIRETIME");
        assert_eq!(command_name_upper(CommandId::Dump), b"DUMP");
        assert_eq!(command_name_upper(CommandId::Restore), b"RESTORE");
        assert_eq!(
            command_name_upper(CommandId::RestoreAsking),
            b"RESTORE-ASKING"
        );
        assert_eq!(command_name_upper(CommandId::Getdel), b"GETDEL");
        assert_eq!(command_name_upper(CommandId::Getset), b"GETSET");
        assert_eq!(command_name_upper(CommandId::Psetex), b"PSETEX");
        assert_eq!(command_name_upper(CommandId::Append), b"APPEND");
        assert_eq!(command_name_upper(CommandId::Getex), b"GETEX");
        assert_eq!(command_name_upper(CommandId::Incrbyfloat), b"INCRBYFLOAT");
        assert_eq!(command_name_upper(CommandId::Msetnx), b"MSETNX");
        assert_eq!(command_name_upper(CommandId::Pfadd), b"PFADD");
        assert_eq!(command_name_upper(CommandId::Pfcount), b"PFCOUNT");
        assert_eq!(command_name_upper(CommandId::Pfmerge), b"PFMERGE");
        assert_eq!(command_name_upper(CommandId::Pfdebug), b"PFDEBUG");
        assert_eq!(command_name_upper(CommandId::Pfselftest), b"PFSELFTEST");
        assert_eq!(command_name_upper(CommandId::Llen), b"LLEN");
        assert_eq!(command_name_upper(CommandId::Lindex), b"LINDEX");
        assert_eq!(command_name_upper(CommandId::Lpos), b"LPOS");
        assert_eq!(command_name_upper(CommandId::Lset), b"LSET");
        assert_eq!(command_name_upper(CommandId::Ltrim), b"LTRIM");
        assert_eq!(command_name_upper(CommandId::Lpushx), b"LPUSHX");
        assert_eq!(command_name_upper(CommandId::Rpushx), b"RPUSHX");
        assert_eq!(command_name_upper(CommandId::Lrem), b"LREM");
        assert_eq!(command_name_upper(CommandId::Linsert), b"LINSERT");
        assert_eq!(command_name_upper(CommandId::Lmove), b"LMOVE");
        assert_eq!(command_name_upper(CommandId::Rpoplpush), b"RPOPLPUSH");
        assert_eq!(command_name_upper(CommandId::Lmpop), b"LMPOP");
        assert_eq!(command_name_upper(CommandId::Blmpop), b"BLMPOP");
        assert_eq!(command_name_upper(CommandId::Blpop), b"BLPOP");
        assert_eq!(command_name_upper(CommandId::Brpop), b"BRPOP");
        assert_eq!(command_name_upper(CommandId::Blmove), b"BLMOVE");
        assert_eq!(command_name_upper(CommandId::Brpoplpush), b"BRPOPLPUSH");
        assert_eq!(command_name_upper(CommandId::Scard), b"SCARD");
        assert_eq!(command_name_upper(CommandId::Smismember), b"SMISMEMBER");
        assert_eq!(command_name_upper(CommandId::Srandmember), b"SRANDMEMBER");
        assert_eq!(command_name_upper(CommandId::Spop), b"SPOP");
        assert_eq!(command_name_upper(CommandId::Smove), b"SMOVE");
        assert_eq!(command_name_upper(CommandId::Sdiff), b"SDIFF");
        assert_eq!(command_name_upper(CommandId::Sdiffstore), b"SDIFFSTORE");
        assert_eq!(command_name_upper(CommandId::Sinter), b"SINTER");
        assert_eq!(command_name_upper(CommandId::Sintercard), b"SINTERCARD");
        assert_eq!(command_name_upper(CommandId::Sinterstore), b"SINTERSTORE");
        assert_eq!(command_name_upper(CommandId::Sunion), b"SUNION");
        assert_eq!(command_name_upper(CommandId::Sunionstore), b"SUNIONSTORE");
        assert_eq!(command_name_upper(CommandId::Zcard), b"ZCARD");
        assert_eq!(command_name_upper(CommandId::Zcount), b"ZCOUNT");
        assert_eq!(command_name_upper(CommandId::Zrevrange), b"ZREVRANGE");
        assert_eq!(
            command_name_upper(CommandId::Zrangebyscore),
            b"ZRANGEBYSCORE"
        );
        assert_eq!(
            command_name_upper(CommandId::Zrevrangebyscore),
            b"ZREVRANGEBYSCORE"
        );
        assert_eq!(command_name_upper(CommandId::Zrank), b"ZRANK");
        assert_eq!(command_name_upper(CommandId::Zrevrank), b"ZREVRANK");
        assert_eq!(command_name_upper(CommandId::Zincrby), b"ZINCRBY");
        assert_eq!(
            command_name_upper(CommandId::Zremrangebyrank),
            b"ZREMRANGEBYRANK"
        );
        assert_eq!(
            command_name_upper(CommandId::Zremrangebyscore),
            b"ZREMRANGEBYSCORE"
        );
        assert_eq!(command_name_upper(CommandId::Zmscore), b"ZMSCORE");
        assert_eq!(command_name_upper(CommandId::Zrandmember), b"ZRANDMEMBER");
        assert_eq!(command_name_upper(CommandId::Zpopmin), b"ZPOPMIN");
        assert_eq!(command_name_upper(CommandId::Zpopmax), b"ZPOPMAX");
        assert_eq!(command_name_upper(CommandId::Bzpopmin), b"BZPOPMIN");
        assert_eq!(command_name_upper(CommandId::Bzpopmax), b"BZPOPMAX");
        assert_eq!(command_name_upper(CommandId::Zdiff), b"ZDIFF");
        assert_eq!(command_name_upper(CommandId::Zdiffstore), b"ZDIFFSTORE");
        assert_eq!(command_name_upper(CommandId::Zinter), b"ZINTER");
        assert_eq!(command_name_upper(CommandId::Zinterstore), b"ZINTERSTORE");
        assert_eq!(command_name_upper(CommandId::Zlexcount), b"ZLEXCOUNT");
        assert_eq!(command_name_upper(CommandId::Zrangestore), b"ZRANGESTORE");
        assert_eq!(command_name_upper(CommandId::Zrangebylex), b"ZRANGEBYLEX");
        assert_eq!(
            command_name_upper(CommandId::Zrevrangebylex),
            b"ZREVRANGEBYLEX"
        );
        assert_eq!(
            command_name_upper(CommandId::Zremrangebylex),
            b"ZREMRANGEBYLEX"
        );
        assert_eq!(command_name_upper(CommandId::Zintercard), b"ZINTERCARD");
        assert_eq!(command_name_upper(CommandId::Zmpop), b"ZMPOP");
        assert_eq!(command_name_upper(CommandId::Bzmpop), b"BZMPOP");
        assert_eq!(command_name_upper(CommandId::Zunion), b"ZUNION");
        assert_eq!(command_name_upper(CommandId::Zunionstore), b"ZUNIONSTORE");
        assert_eq!(command_name_upper(CommandId::Xlen), b"XLEN");
        assert_eq!(command_name_upper(CommandId::Xrange), b"XRANGE");
        assert_eq!(command_name_upper(CommandId::Xrevrange), b"XREVRANGE");
        assert_eq!(command_name_upper(CommandId::Xtrim), b"XTRIM");
        assert_eq!(command_name_upper(CommandId::Xread), b"XREAD");
        assert_eq!(command_name_upper(CommandId::Xack), b"XACK");
        assert_eq!(command_name_upper(CommandId::Xpending), b"XPENDING");
        assert_eq!(command_name_upper(CommandId::Xclaim), b"XCLAIM");
        assert_eq!(command_name_upper(CommandId::Xautoclaim), b"XAUTOCLAIM");
        assert_eq!(command_name_upper(CommandId::Xsetid), b"XSETID");
        assert_eq!(command_name_upper(CommandId::Quit), b"QUIT");
        assert_eq!(command_name_upper(CommandId::Time), b"TIME");
        assert_eq!(command_name_upper(CommandId::Touch), b"TOUCH");
        assert_eq!(command_name_upper(CommandId::Unlink), b"UNLINK");
        assert_eq!(command_name_upper(CommandId::Move), b"MOVE");
        assert_eq!(command_name_upper(CommandId::Swapdb), b"SWAPDB");
        assert_eq!(command_name_upper(CommandId::Latency), b"LATENCY");
        assert_eq!(command_name_upper(CommandId::Module), b"MODULE");
        assert_eq!(command_name_upper(CommandId::Slowlog), b"SLOWLOG");
        assert_eq!(command_name_upper(CommandId::Scan), b"SCAN");
        assert_eq!(command_name_upper(CommandId::Hscan), b"HSCAN");
        assert_eq!(command_name_upper(CommandId::Sscan), b"SSCAN");
        assert_eq!(command_name_upper(CommandId::Zscan), b"ZSCAN");
        assert_eq!(command_name_upper(CommandId::Watch), b"WATCH");
        assert_eq!(command_name_upper(CommandId::Mget), b"MGET");
        assert_eq!(command_name_upper(CommandId::Unknown), b"UNKNOWN");
    }

    #[test]
    fn command_response_names_match_dispatch_behavior() {
        for name in command_names_for_command_response() {
            let dispatch = dispatch_command_name(name);
            let protocol_passthrough = *name == b"REPLICAOF"
                || *name == b"SLAVEOF"
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
