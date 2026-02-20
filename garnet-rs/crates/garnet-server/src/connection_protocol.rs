use crate::command_spec::command_name_upper;
use crate::CommandId;

pub(crate) fn append_simple_string(output: &mut Vec<u8>, value: &[u8]) {
    output.push(b'+');
    output.extend_from_slice(value);
    output.extend_from_slice(b"\r\n");
}

pub(crate) fn append_error_line(output: &mut Vec<u8>, value: &[u8]) {
    output.push(b'-');
    output.extend_from_slice(value);
    output.extend_from_slice(b"\r\n");
}

fn append_wrong_arity_error(output: &mut Vec<u8>, command_upper: &[u8]) {
    output.extend_from_slice(b"-ERR wrong number of arguments for '");
    output.extend_from_slice(command_upper);
    output.extend_from_slice(b"' command\r\n");
}

pub(crate) fn append_wrong_arity_error_for_command(output: &mut Vec<u8>, command: CommandId) {
    append_wrong_arity_error(output, command_name_upper(command));
}

pub(crate) fn ascii_eq_ignore_case(input: &[u8], expected_upper: &[u8]) -> bool {
    input.len() == expected_upper.len()
        && input
            .iter()
            .zip(expected_upper.iter())
            .all(|(lhs, rhs)| ascii_upper(*lhs) == *rhs)
}

fn ascii_upper(value: u8) -> u8 {
    if value.is_ascii_lowercase() {
        value - 32
    } else {
        value
    }
}

pub(crate) fn parse_u16_ascii(value: &[u8]) -> Option<u16> {
    let parsed = std::str::from_utf8(value).ok()?.parse::<u32>().ok()?;
    if parsed <= u16::MAX as u32 {
        Some(parsed as u16)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ascii_eq_ignore_case_matches_case_insensitively() {
        assert!(ascii_eq_ignore_case(b"ping", b"PING"));
        assert!(ascii_eq_ignore_case(b"PING", b"PING"));
        assert!(!ascii_eq_ignore_case(b"pin", b"PING"));
    }

    #[test]
    fn parse_u16_ascii_accepts_bounds_and_rejects_invalid_values() {
        assert_eq!(parse_u16_ascii(b"0"), Some(0));
        assert_eq!(parse_u16_ascii(b"65535"), Some(65535));
        assert_eq!(parse_u16_ascii(b"65536"), None);
        assert_eq!(parse_u16_ascii(b"-1"), None);
        assert_eq!(parse_u16_ascii(b"abc"), None);
    }

    #[test]
    fn wrong_arity_error_uses_uppercase_command_name() {
        let mut output = Vec::new();
        append_wrong_arity_error_for_command(&mut output, CommandId::Asking);
        assert_eq!(
            output,
            b"-ERR wrong number of arguments for 'ASKING' command\r\n"
        );
    }
}
