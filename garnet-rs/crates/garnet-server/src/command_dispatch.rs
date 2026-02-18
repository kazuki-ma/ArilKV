//! RESP command-name dispatch helpers.
//!
//! Fast path uses fixed byte-pattern checks for hot commands.

use garnet_common::ArgSlice;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    Ping,
    Echo,
    Info,
    Dbsize,
    Command,
    Unknown,
}

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
    dispatch_command_name_slow(command)
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

fn dispatch_command_name_slow(command: &[u8]) -> CommandId {
    if is_ascii_eq_4(command, b"DECR") {
        return CommandId::Decr;
    }
    if is_ascii_eq_6(command, b"EXPIRE") {
        return CommandId::Expire;
    }
    if is_ascii_eq_3(command, b"TTL") {
        return CommandId::Ttl;
    }
    if is_ascii_eq_7(command, b"PEXPIRE") {
        return CommandId::Pexpire;
    }
    if is_ascii_eq_4(command, b"PTTL") {
        return CommandId::Pttl;
    }
    if is_ascii_eq_4(command, b"PING") {
        return CommandId::Ping;
    }
    if is_ascii_eq_4(command, b"ECHO") {
        return CommandId::Echo;
    }
    if is_ascii_eq_4(command, b"INFO") {
        return CommandId::Info;
    }
    if is_ascii_eq_6(command, b"DBSIZE") {
        return CommandId::Dbsize;
    }
    if is_ascii_eq_7(command, b"COMMAND") {
        return CommandId::Command;
    }
    CommandId::Unknown
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

fn is_ascii_eq_6(input: &[u8], expected_upper: &[u8; 6]) -> bool {
    input.len() == 6
        && ascii_upper(input[0]) == expected_upper[0]
        && ascii_upper(input[1]) == expected_upper[1]
        && ascii_upper(input[2]) == expected_upper[2]
        && ascii_upper(input[3]) == expected_upper[3]
        && ascii_upper(input[4]) == expected_upper[4]
        && ascii_upper(input[5]) == expected_upper[5]
}

fn is_ascii_eq_7(input: &[u8], expected_upper: &[u8; 7]) -> bool {
    input.len() == 7
        && ascii_upper(input[0]) == expected_upper[0]
        && ascii_upper(input[1]) == expected_upper[1]
        && ascii_upper(input[2]) == expected_upper[2]
        && ascii_upper(input[3]) == expected_upper[3]
        && ascii_upper(input[4]) == expected_upper[4]
        && ascii_upper(input[5]) == expected_upper[5]
        && ascii_upper(input[6]) == expected_upper[6]
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
        assert_eq!(dispatch_command_name(b"dbsize"), CommandId::Dbsize);
        assert_eq!(dispatch_command_name(b"command"), CommandId::Command);
        assert_eq!(dispatch_command_name(b"DECR"), CommandId::Decr);
        assert_eq!(dispatch_command_name(b"EXPIRE"), CommandId::Expire);
        assert_eq!(dispatch_command_name(b"ttl"), CommandId::Ttl);
        assert_eq!(dispatch_command_name(b"pexpire"), CommandId::Pexpire);
        assert_eq!(dispatch_command_name(b"PTTL"), CommandId::Pttl);
    }

    #[test]
    fn unknown_command_maps_to_unknown() {
        assert_eq!(dispatch_command_name(b"HELLO"), CommandId::Unknown);
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
