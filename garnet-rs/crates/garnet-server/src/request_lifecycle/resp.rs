pub(super) fn ascii_eq_ignore_case(input: &[u8], expected_upper: &[u8]) -> bool {
    if input.len() != expected_upper.len() {
        return false;
    }
    input
        .iter()
        .zip(expected_upper.iter())
        .all(|(lhs, rhs)| lhs.to_ascii_uppercase() == *rhs)
}

pub(super) fn append_simple_string(response_out: &mut Vec<u8>, value: &[u8]) {
    response_out.push(b'+');
    response_out.extend_from_slice(value);
    response_out.extend_from_slice(b"\r\n");
}

pub(super) fn append_error(response_out: &mut Vec<u8>, message: &[u8]) {
    response_out.push(b'-');
    response_out.extend_from_slice(message);
    response_out.extend_from_slice(b"\r\n");
}

pub(super) fn append_bulk_string(response_out: &mut Vec<u8>, value: &[u8]) {
    response_out.extend_from_slice(b"$");
    response_out.extend_from_slice(value.len().to_string().as_bytes());
    response_out.extend_from_slice(b"\r\n");
    response_out.extend_from_slice(value);
    response_out.extend_from_slice(b"\r\n");
}

pub(super) fn append_bulk_array(response_out: &mut Vec<u8>, items: &[&[u8]]) {
    response_out.push(b'*');
    response_out.extend_from_slice(items.len().to_string().as_bytes());
    response_out.extend_from_slice(b"\r\n");
    for item in items {
        append_bulk_string(response_out, item);
    }
}

pub(super) fn append_array_length(response_out: &mut Vec<u8>, len: usize) {
    response_out.push(b'*');
    response_out.extend_from_slice(len.to_string().as_bytes());
    response_out.extend_from_slice(b"\r\n");
}

/// Emit RESP3 map length prefix: `%<count>\r\n`.
pub(super) fn append_map_length(response_out: &mut Vec<u8>, len: usize) {
    response_out.push(b'%');
    response_out.extend_from_slice(len.to_string().as_bytes());
    response_out.extend_from_slice(b"\r\n");
}

/// Emit RESP3 set length prefix: `~<count>\r\n`.
pub(super) fn append_set_length(response_out: &mut Vec<u8>, len: usize) {
    response_out.push(b'~');
    response_out.extend_from_slice(len.to_string().as_bytes());
    response_out.extend_from_slice(b"\r\n");
}

/// Emit RESP3 double value: `,<value>\r\n`.
pub(super) fn append_double(response_out: &mut Vec<u8>, value: f64) {
    response_out.push(b',');
    if value == f64::INFINITY {
        response_out.extend_from_slice(b"inf");
    } else if value == f64::NEG_INFINITY {
        response_out.extend_from_slice(b"-inf");
    } else {
        response_out.extend_from_slice(value.to_string().as_bytes());
    }
    response_out.extend_from_slice(b"\r\n");
}

/// Emit RESP3 push length prefix: `><count>\r\n`.
pub(super) fn append_push_length(response_out: &mut Vec<u8>, len: usize) {
    response_out.push(b'>');
    response_out.extend_from_slice(len.to_string().as_bytes());
    response_out.extend_from_slice(b"\r\n");
}

pub(super) fn append_null_bulk_string(response_out: &mut Vec<u8>) {
    response_out.extend_from_slice(b"$-1\r\n");
}

pub(super) fn append_null_array(response_out: &mut Vec<u8>) {
    response_out.extend_from_slice(b"*-1\r\n");
}

pub(super) fn append_null(response_out: &mut Vec<u8>) {
    response_out.extend_from_slice(b"_\r\n");
}

/// Emit RESP3 verbatim string: `=<payload_len>\r\n<format>:<value>\r\n`.
pub(super) fn append_verbatim_string(response_out: &mut Vec<u8>, format: &[u8], value: &[u8]) {
    response_out.push(b'=');
    let payload_len = format.len().saturating_add(1).saturating_add(value.len());
    response_out.extend_from_slice(payload_len.to_string().as_bytes());
    response_out.extend_from_slice(b"\r\n");
    response_out.extend_from_slice(format);
    response_out.push(b':');
    response_out.extend_from_slice(value);
    response_out.extend_from_slice(b"\r\n");
}

pub(super) fn append_integer(response_out: &mut Vec<u8>, value: i64) {
    response_out.push(b':');
    response_out.extend_from_slice(value.to_string().as_bytes());
    response_out.extend_from_slice(b"\r\n");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ascii_eq_ignore_case_matches_exact_and_case_insensitive_values() {
        assert!(ascii_eq_ignore_case(b"ping", b"PING"));
        assert!(ascii_eq_ignore_case(b"PING", b"PING"));
        assert!(!ascii_eq_ignore_case(b"PINGX", b"PING"));
    }
}
