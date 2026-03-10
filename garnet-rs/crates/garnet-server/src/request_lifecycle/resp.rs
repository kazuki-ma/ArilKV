pub(super) fn ascii_eq_ignore_case(input: &[u8], expected_upper: &[u8]) -> bool {
    if input.len() != expected_upper.len() {
        return false;
    }
    input
        .iter()
        .zip(expected_upper.iter())
        .all(|(lhs, rhs)| lhs.to_ascii_uppercase() == *rhs)
}

fn append_u64_ascii(response_out: &mut Vec<u8>, mut value: u64) {
    let mut digits = [0u8; 20];
    let mut cursor = digits.len();
    loop {
        cursor -= 1;
        digits[cursor] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    response_out.extend_from_slice(&digits[cursor..]);
}

fn append_usize_ascii(response_out: &mut Vec<u8>, value: usize) {
    append_u64_ascii(response_out, value as u64);
}

fn append_i64_ascii(response_out: &mut Vec<u8>, value: i64) {
    if value < 0 {
        response_out.push(b'-');
        append_u64_ascii(response_out, value.wrapping_neg() as u64);
    } else {
        append_u64_ascii(response_out, value as u64);
    }
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
    append_usize_ascii(response_out, value.len());
    response_out.extend_from_slice(b"\r\n");
    response_out.extend_from_slice(value);
    response_out.extend_from_slice(b"\r\n");
}

pub(super) fn format_resp_double(value: f64) -> String {
    if value == f64::INFINITY {
        return "inf".to_string();
    }
    if value == f64::NEG_INFINITY {
        return "-inf".to_string();
    }
    if value == 0.0 {
        if value.is_sign_negative() {
            return "-0".to_string();
        }
        return "0".to_string();
    }
    if let Some(integer) = exact_i64_from_double(value) {
        return integer.to_string();
    }

    let mut buffer = ryu::Buffer::new();
    let formatted = buffer.format_finite(value);
    if let Some(exponent_index) = formatted.find('e') {
        let exponent = &formatted[exponent_index + 1..];
        if !exponent.starts_with('+') && !exponent.starts_with('-') {
            let mut result = String::with_capacity(formatted.len() + 1);
            result.push_str(&formatted[..=exponent_index]);
            result.push('+');
            result.push_str(exponent);
            return result;
        }
    }
    formatted.to_string()
}

pub(super) fn append_bulk_double(response_out: &mut Vec<u8>, value: f64) {
    let formatted = format_resp_double(value);
    append_bulk_string(response_out, formatted.as_bytes());
}

fn exact_i64_from_double(value: f64) -> Option<i64> {
    const MIN_SAFE_I64_AS_F64: f64 = -(i64::MAX as f64) / 2.0;
    const MAX_SAFE_I64_AS_F64: f64 = (i64::MAX as f64) / 2.0;
    if !(MIN_SAFE_I64_AS_F64..=MAX_SAFE_I64_AS_F64).contains(&value) {
        return None;
    }
    let integer = value as i64;
    if integer as f64 == value {
        return Some(integer);
    }
    None
}

pub(super) fn append_bulk_array(response_out: &mut Vec<u8>, items: &[&[u8]]) {
    response_out.push(b'*');
    append_usize_ascii(response_out, items.len());
    response_out.extend_from_slice(b"\r\n");
    for item in items {
        append_bulk_string(response_out, item);
    }
}

pub(super) fn append_array_length(response_out: &mut Vec<u8>, len: usize) {
    response_out.push(b'*');
    append_usize_ascii(response_out, len);
    response_out.extend_from_slice(b"\r\n");
}

/// Emit RESP3 map length prefix: `%<count>\r\n`.
pub(super) fn append_map_length(response_out: &mut Vec<u8>, len: usize) {
    response_out.push(b'%');
    append_usize_ascii(response_out, len);
    response_out.extend_from_slice(b"\r\n");
}

/// Emit RESP3 set length prefix: `~<count>\r\n`.
pub(super) fn append_set_length(response_out: &mut Vec<u8>, len: usize) {
    response_out.push(b'~');
    append_usize_ascii(response_out, len);
    response_out.extend_from_slice(b"\r\n");
}

/// Emit RESP3 double value: `,<value>\r\n`.
pub(super) fn append_double(response_out: &mut Vec<u8>, value: f64) {
    response_out.push(b',');
    let formatted = format_resp_double(value);
    response_out.extend_from_slice(formatted.as_bytes());
    response_out.extend_from_slice(b"\r\n");
}

/// Emit RESP3 boolean value: `#t\r\n` or `#f\r\n`.
pub(super) fn append_resp3_boolean(response_out: &mut Vec<u8>, value: bool) {
    if value {
        response_out.extend_from_slice(b"#t\r\n");
    } else {
        response_out.extend_from_slice(b"#f\r\n");
    }
}

/// Emit RESP3 big number value: `(<value>\r\n`.
pub(super) fn append_resp3_bignum(response_out: &mut Vec<u8>, value: &[u8]) {
    response_out.push(b'(');
    response_out.extend_from_slice(value);
    response_out.extend_from_slice(b"\r\n");
}

/// Emit RESP3 push length prefix: `><count>\r\n`.
pub(super) fn append_push_length(response_out: &mut Vec<u8>, len: usize) {
    response_out.push(b'>');
    append_usize_ascii(response_out, len);
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
    append_usize_ascii(response_out, payload_len);
    response_out.extend_from_slice(b"\r\n");
    response_out.extend_from_slice(format);
    response_out.push(b':');
    response_out.extend_from_slice(value);
    response_out.extend_from_slice(b"\r\n");
}

pub(super) fn append_integer(response_out: &mut Vec<u8>, value: i64) {
    response_out.push(b':');
    append_i64_ascii(response_out, value);
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

    #[test]
    fn append_integer_handles_negative_and_min_values() {
        let mut response = Vec::new();
        append_integer(&mut response, 42);
        assert_eq!(response, b":42\r\n");

        response.clear();
        append_integer(&mut response, -7);
        assert_eq!(response, b":-7\r\n");

        response.clear();
        append_integer(&mut response, i64::MIN);
        assert_eq!(response, b":-9223372036854775808\r\n");
    }

    #[test]
    fn append_length_prefixes_emit_expected_ascii() {
        let mut response = Vec::new();
        append_array_length(&mut response, 0);
        append_map_length(&mut response, 12);
        append_set_length(&mut response, 345);
        append_push_length(&mut response, 6789);
        assert_eq!(response, b"*0\r\n%12\r\n~345\r\n>6789\r\n");
    }
}
