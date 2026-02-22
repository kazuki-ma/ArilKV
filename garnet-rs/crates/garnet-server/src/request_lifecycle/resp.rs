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

pub(super) fn append_null_bulk_string(response_out: &mut Vec<u8>) {
    response_out.extend_from_slice(b"$-1\r\n");
}

pub(super) fn append_null_array(response_out: &mut Vec<u8>) {
    response_out.extend_from_slice(b"*-1\r\n");
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
