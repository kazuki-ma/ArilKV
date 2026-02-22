//! RESP command-name dispatch helpers.
//!
//! Fast path uses fixed byte-pattern checks for hot commands.

use crate::command_spec::{command_id_from_name, CommandId};
use garnet_common::ArgSlice;

pub fn dispatch_command_name(command: &[u8]) -> CommandId {
    if is_ascii_eq_3(command, b"GET") {
        return CommandId::Get;
    }
    if is_ascii_eq_3(command, b"SET") {
        return CommandId::Set;
    }
    if is_ascii_eq_3(command, b"DEL") {
        return CommandId::Del;
    }
    if is_ascii_eq_4(command, b"INCR") {
        return CommandId::Incr;
    }
    command_id_from_name(command).unwrap_or(CommandId::Unknown)
}

pub fn dispatch_from_resp_args(args: &[&[u8]]) -> CommandId {
    if args.is_empty() {
        return CommandId::Unknown;
    }
    dispatch_command_name(args[0])
}

/// # Safety
///
/// Callers must guarantee all `ArgSlice` values reference valid memory for the
/// duration of this call.
pub unsafe fn dispatch_from_arg_slices(args: &[ArgSlice]) -> CommandId {
    if args.is_empty() {
        return CommandId::Unknown;
    }
    // SAFETY: guaranteed by caller per function contract.
    let command = unsafe { args[0].as_slice() };
    dispatch_command_name(command)
}

fn ascii_upper(byte: u8) -> u8 {
    if byte.is_ascii_lowercase() {
        byte - 32
    } else {
        byte
    }
}

fn is_ascii_eq_3(input: &[u8], expected_upper: &[u8; 3]) -> bool {
    input.len() == 3
        && ascii_upper(input[0]) == expected_upper[0]
        && ascii_upper(input[1]) == expected_upper[1]
        && ascii_upper(input[2]) == expected_upper[2]
}

fn is_ascii_eq_4(input: &[u8], expected_upper: &[u8; 4]) -> bool {
    input.len() == 4
        && ascii_upper(input[0]) == expected_upper[0]
        && ascii_upper(input[1]) == expected_upper[1]
        && ascii_upper(input[2]) == expected_upper[2]
        && ascii_upper(input[3]) == expected_upper[3]
}

#[cfg(test)]
mod tests {
    use super::*;
    use garnet_common::parse_resp_command_arg_slices;

    #[test]
    fn fast_path_dispatches_hot_commands_case_insensitively() {
        assert_eq!(dispatch_command_name(b"GET"), CommandId::Get);
        assert_eq!(dispatch_command_name(b"set"), CommandId::Set);
        assert_eq!(dispatch_command_name(b"Del"), CommandId::Del);
        assert_eq!(dispatch_command_name(b"inCr"), CommandId::Incr);
    }

    #[test]
    fn slow_path_dispatches_secondary_commands() {
        assert_eq!(dispatch_command_name(b"PING"), CommandId::Ping);
        assert_eq!(dispatch_command_name(b"echo"), CommandId::Echo);
        assert_eq!(dispatch_command_name(b"INFO"), CommandId::Info);
        assert_eq!(dispatch_command_name(b"memory"), CommandId::Memory);
        assert_eq!(dispatch_command_name(b"dbsize"), CommandId::Dbsize);
        assert_eq!(dispatch_command_name(b"DEBUG"), CommandId::Debug);
        assert_eq!(dispatch_command_name(b"object"), CommandId::Object);
        assert_eq!(dispatch_command_name(b"KEYS"), CommandId::Keys);
        assert_eq!(dispatch_command_name(b"randomkey"), CommandId::Randomkey);
        assert_eq!(dispatch_command_name(b"SCAN"), CommandId::Scan);
        assert_eq!(dispatch_command_name(b"hscan"), CommandId::Hscan);
        assert_eq!(dispatch_command_name(b"SSCAN"), CommandId::Sscan);
        assert_eq!(dispatch_command_name(b"zscan"), CommandId::Zscan);
        assert_eq!(dispatch_command_name(b"FLUSHDB"), CommandId::Flushdb);
        assert_eq!(dispatch_command_name(b"flushall"), CommandId::Flushall);
        assert_eq!(dispatch_command_name(b"FUNCTION"), CommandId::Function);
        assert_eq!(dispatch_command_name(b"script"), CommandId::Script);
        assert_eq!(dispatch_command_name(b"CONFIG"), CommandId::Config);
        assert_eq!(dispatch_command_name(b"command"), CommandId::Command);
        assert_eq!(dispatch_command_name(b"DECR"), CommandId::Decr);
        assert_eq!(dispatch_command_name(b"RENAME"), CommandId::Rename);
        assert_eq!(dispatch_command_name(b"renamenx"), CommandId::Renamenx);
        assert_eq!(dispatch_command_name(b"COPY"), CommandId::Copy);
        assert_eq!(dispatch_command_name(b"setex"), CommandId::Setex);
        assert_eq!(dispatch_command_name(b"SETNX"), CommandId::Setnx);
        assert_eq!(dispatch_command_name(b"strlen"), CommandId::Strlen);
        assert_eq!(dispatch_command_name(b"GETRANGE"), CommandId::Getrange);
        assert_eq!(dispatch_command_name(b"substr"), CommandId::Substr);
        assert_eq!(dispatch_command_name(b"GETBIT"), CommandId::Getbit);
        assert_eq!(dispatch_command_name(b"setbit"), CommandId::Setbit);
        assert_eq!(dispatch_command_name(b"SETRANGE"), CommandId::Setrange);
        assert_eq!(dispatch_command_name(b"bitcount"), CommandId::Bitcount);
        assert_eq!(dispatch_command_name(b"bitpos"), CommandId::Bitpos);
        assert_eq!(dispatch_command_name(b"BITOP"), CommandId::Bitop);
        assert_eq!(dispatch_command_name(b"APPEND"), CommandId::Append);
        assert_eq!(dispatch_command_name(b"getex"), CommandId::Getex);
        assert_eq!(
            dispatch_command_name(b"INCRBYFLOAT"),
            CommandId::Incrbyfloat
        );
        assert_eq!(dispatch_command_name(b"msetnx"), CommandId::Msetnx);
        assert_eq!(dispatch_command_name(b"QUIT"), CommandId::Quit);
        assert_eq!(dispatch_command_name(b"time"), CommandId::Time);
        assert_eq!(dispatch_command_name(b"TOUCH"), CommandId::Touch);
        assert_eq!(dispatch_command_name(b"unlink"), CommandId::Unlink);
        assert_eq!(dispatch_command_name(b"GETDEL"), CommandId::Getdel);
        assert_eq!(dispatch_command_name(b"getset"), CommandId::Getset);
        assert_eq!(dispatch_command_name(b"PSETEX"), CommandId::Psetex);
        assert_eq!(dispatch_command_name(b"INCRBY"), CommandId::Incrby);
        assert_eq!(dispatch_command_name(b"decrby"), CommandId::Decrby);
        assert_eq!(dispatch_command_name(b"EXISTS"), CommandId::Exists);
        assert_eq!(dispatch_command_name(b"type"), CommandId::Type);
        assert_eq!(dispatch_command_name(b"MGET"), CommandId::Mget);
        assert_eq!(dispatch_command_name(b"mset"), CommandId::Mset);
        assert_eq!(dispatch_command_name(b"EXPIRE"), CommandId::Expire);
        assert_eq!(dispatch_command_name(b"expireat"), CommandId::Expireat);
        assert_eq!(dispatch_command_name(b"EXPIRETIME"), CommandId::Expiretime);
        assert_eq!(dispatch_command_name(b"ttl"), CommandId::Ttl);
        assert_eq!(dispatch_command_name(b"pexpire"), CommandId::Pexpire);
        assert_eq!(dispatch_command_name(b"PEXPIREAT"), CommandId::Pexpireat);
        assert_eq!(
            dispatch_command_name(b"pexpiretime"),
            CommandId::Pexpiretime
        );
        assert_eq!(dispatch_command_name(b"PTTL"), CommandId::Pttl);
        assert_eq!(dispatch_command_name(b"persist"), CommandId::Persist);
        assert_eq!(dispatch_command_name(b"hset"), CommandId::Hset);
        assert_eq!(dispatch_command_name(b"HGET"), CommandId::Hget);
        assert_eq!(dispatch_command_name(b"hdel"), CommandId::Hdel);
        assert_eq!(dispatch_command_name(b"HGETALL"), CommandId::Hgetall);
        assert_eq!(dispatch_command_name(b"hlen"), CommandId::Hlen);
        assert_eq!(dispatch_command_name(b"HMGET"), CommandId::Hmget);
        assert_eq!(dispatch_command_name(b"hmset"), CommandId::Hmset);
        assert_eq!(dispatch_command_name(b"HSETNX"), CommandId::Hsetnx);
        assert_eq!(dispatch_command_name(b"hexists"), CommandId::Hexists);
        assert_eq!(dispatch_command_name(b"HKEYS"), CommandId::Hkeys);
        assert_eq!(dispatch_command_name(b"hvals"), CommandId::Hvals);
        assert_eq!(dispatch_command_name(b"HSTRLEN"), CommandId::Hstrlen);
        assert_eq!(dispatch_command_name(b"hincrby"), CommandId::Hincrby);
        assert_eq!(
            dispatch_command_name(b"HINCRBYFLOAT"),
            CommandId::Hincrbyfloat
        );
        assert_eq!(dispatch_command_name(b"hrandfield"), CommandId::Hrandfield);
        assert_eq!(dispatch_command_name(b"lpush"), CommandId::Lpush);
        assert_eq!(dispatch_command_name(b"RPUSH"), CommandId::Rpush);
        assert_eq!(dispatch_command_name(b"lpop"), CommandId::Lpop);
        assert_eq!(dispatch_command_name(b"RPOP"), CommandId::Rpop);
        assert_eq!(dispatch_command_name(b"lrange"), CommandId::Lrange);
        assert_eq!(dispatch_command_name(b"LLEN"), CommandId::Llen);
        assert_eq!(dispatch_command_name(b"lindex"), CommandId::Lindex);
        assert_eq!(dispatch_command_name(b"LPOS"), CommandId::Lpos);
        assert_eq!(dispatch_command_name(b"LSET"), CommandId::Lset);
        assert_eq!(dispatch_command_name(b"ltrim"), CommandId::Ltrim);
        assert_eq!(dispatch_command_name(b"LPUSHX"), CommandId::Lpushx);
        assert_eq!(dispatch_command_name(b"rpushx"), CommandId::Rpushx);
        assert_eq!(dispatch_command_name(b"LREM"), CommandId::Lrem);
        assert_eq!(dispatch_command_name(b"linsert"), CommandId::Linsert);
        assert_eq!(dispatch_command_name(b"LMOVE"), CommandId::Lmove);
        assert_eq!(dispatch_command_name(b"rpoplpush"), CommandId::Rpoplpush);
        assert_eq!(dispatch_command_name(b"lmpop"), CommandId::Lmpop);
        assert_eq!(dispatch_command_name(b"BLMPOP"), CommandId::Blmpop);
        assert_eq!(dispatch_command_name(b"blpop"), CommandId::Blpop);
        assert_eq!(dispatch_command_name(b"BRPOP"), CommandId::Brpop);
        assert_eq!(dispatch_command_name(b"blmove"), CommandId::Blmove);
        assert_eq!(dispatch_command_name(b"BRPOPLPUSH"), CommandId::Brpoplpush);
        assert_eq!(dispatch_command_name(b"sadd"), CommandId::Sadd);
        assert_eq!(dispatch_command_name(b"SREM"), CommandId::Srem);
        assert_eq!(dispatch_command_name(b"smembers"), CommandId::Smembers);
        assert_eq!(dispatch_command_name(b"SISMEMBER"), CommandId::Sismember);
        assert_eq!(dispatch_command_name(b"SCARD"), CommandId::Scard);
        assert_eq!(dispatch_command_name(b"smismember"), CommandId::Smismember);
        assert_eq!(
            dispatch_command_name(b"SRANDMEMBER"),
            CommandId::Srandmember
        );
        assert_eq!(dispatch_command_name(b"spop"), CommandId::Spop);
        assert_eq!(dispatch_command_name(b"SMOVE"), CommandId::Smove);
        assert_eq!(dispatch_command_name(b"SDIFF"), CommandId::Sdiff);
        assert_eq!(dispatch_command_name(b"sdiffstore"), CommandId::Sdiffstore);
        assert_eq!(dispatch_command_name(b"SINTER"), CommandId::Sinter);
        assert_eq!(dispatch_command_name(b"sintercard"), CommandId::Sintercard);
        assert_eq!(
            dispatch_command_name(b"sinterstore"),
            CommandId::Sinterstore
        );
        assert_eq!(dispatch_command_name(b"SUNION"), CommandId::Sunion);
        assert_eq!(
            dispatch_command_name(b"sunionstore"),
            CommandId::Sunionstore
        );
        assert_eq!(dispatch_command_name(b"zadd"), CommandId::Zadd);
        assert_eq!(dispatch_command_name(b"ZREM"), CommandId::Zrem);
        assert_eq!(dispatch_command_name(b"zrange"), CommandId::Zrange);
        assert_eq!(dispatch_command_name(b"ZREVRANGE"), CommandId::Zrevrange);
        assert_eq!(
            dispatch_command_name(b"zrangebyscore"),
            CommandId::Zrangebyscore
        );
        assert_eq!(
            dispatch_command_name(b"ZREVRANGEBYSCORE"),
            CommandId::Zrevrangebyscore
        );
        assert_eq!(dispatch_command_name(b"ZSCORE"), CommandId::Zscore);
        assert_eq!(dispatch_command_name(b"ZCARD"), CommandId::Zcard);
        assert_eq!(dispatch_command_name(b"zcount"), CommandId::Zcount);
        assert_eq!(dispatch_command_name(b"ZRANK"), CommandId::Zrank);
        assert_eq!(dispatch_command_name(b"zrevrank"), CommandId::Zrevrank);
        assert_eq!(dispatch_command_name(b"ZINCRBY"), CommandId::Zincrby);
        assert_eq!(
            dispatch_command_name(b"zremrangebyrank"),
            CommandId::Zremrangebyrank
        );
        assert_eq!(
            dispatch_command_name(b"ZREMRANGEBYSCORE"),
            CommandId::Zremrangebyscore
        );
        assert_eq!(dispatch_command_name(b"ZMSCORE"), CommandId::Zmscore);
        assert_eq!(
            dispatch_command_name(b"zrandmember"),
            CommandId::Zrandmember
        );
        assert_eq!(dispatch_command_name(b"ZPOPMIN"), CommandId::Zpopmin);
        assert_eq!(dispatch_command_name(b"zpopmax"), CommandId::Zpopmax);
        assert_eq!(dispatch_command_name(b"BZPOPMIN"), CommandId::Bzpopmin);
        assert_eq!(dispatch_command_name(b"bzpopmax"), CommandId::Bzpopmax);
        assert_eq!(dispatch_command_name(b"ZDIFF"), CommandId::Zdiff);
        assert_eq!(dispatch_command_name(b"zdiffstore"), CommandId::Zdiffstore);
        assert_eq!(dispatch_command_name(b"ZINTER"), CommandId::Zinter);
        assert_eq!(
            dispatch_command_name(b"zinterstore"),
            CommandId::Zinterstore
        );
        assert_eq!(dispatch_command_name(b"ZLEXCOUNT"), CommandId::Zlexcount);
        assert_eq!(
            dispatch_command_name(b"zrangestore"),
            CommandId::Zrangestore
        );
        assert_eq!(
            dispatch_command_name(b"zrangebylex"),
            CommandId::Zrangebylex
        );
        assert_eq!(
            dispatch_command_name(b"ZREVRANGEBYLEX"),
            CommandId::Zrevrangebylex
        );
        assert_eq!(
            dispatch_command_name(b"zremrangebylex"),
            CommandId::Zremrangebylex
        );
        assert_eq!(dispatch_command_name(b"ZINTERCARD"), CommandId::Zintercard);
        assert_eq!(dispatch_command_name(b"zmpop"), CommandId::Zmpop);
        assert_eq!(dispatch_command_name(b"BZMPOP"), CommandId::Bzmpop);
        assert_eq!(dispatch_command_name(b"zunion"), CommandId::Zunion);
        assert_eq!(
            dispatch_command_name(b"ZUNIONSTORE"),
            CommandId::Zunionstore
        );
        assert_eq!(dispatch_command_name(b"xadd"), CommandId::Xadd);
        assert_eq!(dispatch_command_name(b"XDEL"), CommandId::Xdel);
        assert_eq!(dispatch_command_name(b"xgroup"), CommandId::Xgroup);
        assert_eq!(dispatch_command_name(b"XREADGROUP"), CommandId::Xreadgroup);
        assert_eq!(dispatch_command_name(b"xinfo"), CommandId::Xinfo);
        assert_eq!(dispatch_command_name(b"XLEN"), CommandId::Xlen);
        assert_eq!(dispatch_command_name(b"xrange"), CommandId::Xrange);
        assert_eq!(dispatch_command_name(b"XREVRANGE"), CommandId::Xrevrange);
        assert_eq!(dispatch_command_name(b"multi"), CommandId::Multi);
        assert_eq!(dispatch_command_name(b"EXEC"), CommandId::Exec);
        assert_eq!(dispatch_command_name(b"discard"), CommandId::Discard);
        assert_eq!(dispatch_command_name(b"watch"), CommandId::Watch);
        assert_eq!(dispatch_command_name(b"UNWATCH"), CommandId::Unwatch);
        assert_eq!(dispatch_command_name(b"asking"), CommandId::Asking);
        assert_eq!(dispatch_command_name(b"HELLO"), CommandId::Hello);
        assert_eq!(dispatch_command_name(b"LASTSAVE"), CommandId::Lastsave);
        assert_eq!(dispatch_command_name(b"auth"), CommandId::Auth);
        assert_eq!(dispatch_command_name(b"SELECT"), CommandId::Select);
        assert_eq!(dispatch_command_name(b"client"), CommandId::Client);
        assert_eq!(dispatch_command_name(b"ROLE"), CommandId::Role);
        assert_eq!(dispatch_command_name(b"wait"), CommandId::Wait);
        assert_eq!(dispatch_command_name(b"WAITAOF"), CommandId::Waitaof);
        assert_eq!(dispatch_command_name(b"save"), CommandId::Save);
        assert_eq!(dispatch_command_name(b"BGSAVE"), CommandId::Bgsave);
        assert_eq!(
            dispatch_command_name(b"bgrewriteaof"),
            CommandId::Bgrewriteaof
        );
        assert_eq!(dispatch_command_name(b"readonly"), CommandId::Readonly);
        assert_eq!(dispatch_command_name(b"READWRITE"), CommandId::Readwrite);
        assert_eq!(dispatch_command_name(b"reset"), CommandId::Reset);
        assert_eq!(dispatch_command_name(b"LOLWUT"), CommandId::Lolwut);
    }

    #[test]
    fn unknown_command_maps_to_unknown() {
        assert_eq!(dispatch_command_name(b"HELLOX"), CommandId::Unknown);
    }

    #[test]
    fn dispatch_from_args_handles_empty_input() {
        assert_eq!(dispatch_from_resp_args(&[]), CommandId::Unknown);
    }

    #[test]
    fn dispatch_from_arg_slices_uses_first_argument() {
        let frame = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        let mut args = [ArgSlice::EMPTY; 2];
        parse_resp_command_arg_slices(frame, &mut args).unwrap();

        // SAFETY: `args` references `frame`, which is alive in this scope.
        let command = unsafe { dispatch_from_arg_slices(&args[..2]) };
        assert_eq!(command, CommandId::Get);
    }
}
