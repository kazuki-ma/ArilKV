//! RESP protocol parsing helpers.
//!
//! This parser focuses on command frames of shape:
//! `*<n>\r\n$<len>\r\n<arg>\r\n...`, and returns zero-copy argument slices.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RespCommandMeta {
    pub argument_count: usize,
    pub bytes_consumed: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RespParseError {
    Incomplete,
    InvalidArrayPrefix { found: u8, position: usize },
    InvalidBulkPrefix { found: u8, position: usize },
    InvalidInteger { position: usize },
    IntegerOverflow,
    InvalidCrlf { position: usize },
    NegativeArrayLength { length: i64 },
    NegativeBulkLength { length: i64 },
    ArgumentCapacityExceeded { required: usize, capacity: usize },
}

impl core::fmt::Display for RespParseError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Incomplete => write!(f, "incomplete RESP frame"),
            Self::InvalidArrayPrefix { found, position } => write!(
                f,
                "invalid RESP array prefix '{}' at {}",
                *found as char, position
            ),
            Self::InvalidBulkPrefix { found, position } => write!(
                f,
                "invalid RESP bulk-string prefix '{}' at {}",
                *found as char, position
            ),
            Self::InvalidInteger { position } => {
                write!(f, "invalid RESP integer at byte {}", position)
            }
            Self::IntegerOverflow => write!(f, "RESP integer overflow"),
            Self::InvalidCrlf { position } => write!(f, "invalid CRLF at byte {}", position),
            Self::NegativeArrayLength { length } => {
                write!(f, "negative RESP array length {}", length)
            }
            Self::NegativeBulkLength { length } => {
                write!(f, "negative RESP bulk-string length {}", length)
            }
            Self::ArgumentCapacityExceeded { required, capacity } => write!(
                f,
                "RESP argument capacity exceeded: required {}, capacity {}",
                required, capacity
            ),
        }
    }
}

impl std::error::Error for RespParseError {}

pub fn parse_resp_command<'a>(
    input: &'a [u8],
    args_out: &mut [&'a [u8]],
) -> Result<RespCommandMeta, RespParseError> {
    if input.is_empty() {
        return Err(RespParseError::Incomplete);
    }
    if input[0] != b'*' {
        return Err(RespParseError::InvalidArrayPrefix {
            found: input[0],
            position: 0,
        });
    }

    let (array_len, mut cursor) = parse_i64_until_crlf(input, 1)?;
    if array_len < 0 {
        return Err(RespParseError::NegativeArrayLength { length: array_len });
    }
    let argument_count = array_len as usize;
    if argument_count > args_out.len() {
        return Err(RespParseError::ArgumentCapacityExceeded {
            required: argument_count,
            capacity: args_out.len(),
        });
    }

    for slot in 0..argument_count {
        if cursor >= input.len() {
            return Err(RespParseError::Incomplete);
        }
        if input[cursor] != b'$' {
            return Err(RespParseError::InvalidBulkPrefix {
                found: input[cursor],
                position: cursor,
            });
        }
        cursor += 1;

        let (bulk_len, after_len_cursor) = parse_i64_until_crlf(input, cursor)?;
        cursor = after_len_cursor;
        if bulk_len < 0 {
            return Err(RespParseError::NegativeBulkLength { length: bulk_len });
        }
        let bulk_len = bulk_len as usize;

        let payload_end = cursor
            .checked_add(bulk_len)
            .ok_or(RespParseError::IntegerOverflow)?;
        let frame_end = payload_end
            .checked_add(2)
            .ok_or(RespParseError::IntegerOverflow)?;
        if frame_end > input.len() {
            return Err(RespParseError::Incomplete);
        }

        args_out[slot] = &input[cursor..payload_end];
        if input[payload_end] != b'\r' || input[payload_end + 1] != b'\n' {
            return Err(RespParseError::InvalidCrlf {
                position: payload_end,
            });
        }
        cursor = frame_end;
    }

    Ok(RespCommandMeta {
        argument_count,
        bytes_consumed: cursor,
    })
}

fn parse_i64_until_crlf(input: &[u8], start: usize) -> Result<(i64, usize), RespParseError> {
    if start >= input.len() {
        return Err(RespParseError::Incomplete);
    }

    let mut cursor = start;
    let mut negative = false;
    if input[cursor] == b'-' {
        negative = true;
        cursor += 1;
        if cursor >= input.len() {
            return Err(RespParseError::Incomplete);
        }
    }

    let mut value: i64 = 0;
    let mut saw_digit = false;

    while cursor < input.len() {
        let byte = input[cursor];
        if byte == b'\r' {
            if cursor + 1 >= input.len() {
                return Err(RespParseError::Incomplete);
            }
            if input[cursor + 1] != b'\n' {
                return Err(RespParseError::InvalidCrlf { position: cursor });
            }
            if !saw_digit {
                return Err(RespParseError::InvalidInteger { position: start });
            }
            return Ok((if negative { -value } else { value }, cursor + 2));
        }

        if !byte.is_ascii_digit() {
            return Err(RespParseError::InvalidInteger { position: cursor });
        }
        saw_digit = true;
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add((byte - b'0') as i64))
            .ok_or(RespParseError::IntegerOverflow)?;
        cursor += 1;
    }

    Err(RespParseError::Incomplete)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_resp_command_and_returns_zero_copy_slices() {
        let frame = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        let mut args = [&b""[..]; 4];

        let meta = parse_resp_command(frame, &mut args).unwrap();

        assert_eq!(meta.argument_count, 2);
        assert_eq!(meta.bytes_consumed, frame.len());
        assert_eq!(args[0], b"GET");
        assert_eq!(args[1], b"key");
    }

    #[test]
    fn parses_frame_with_trailing_bytes() {
        let frame = b"*1\r\n$4\r\nPING\r\nJUNK";
        let mut args = [&b""[..]; 2];

        let meta = parse_resp_command(frame, &mut args).unwrap();
        assert_eq!(meta.argument_count, 1);
        assert_eq!(meta.bytes_consumed, 14);
        assert_eq!(args[0], b"PING");
        assert_eq!(&frame[meta.bytes_consumed..], b"JUNK");
    }

    #[test]
    fn returns_incomplete_for_partial_payload() {
        let frame = b"*2\r\n$3\r\nGET\r\n$5\r\nke";
        let mut args = [&b""[..]; 2];

        let err = parse_resp_command(frame, &mut args).err().unwrap();
        assert_eq!(err, RespParseError::Incomplete);
    }

    #[test]
    fn rejects_invalid_prefix() {
        let frame = b"+OK\r\n";
        let mut args = [&b""[..]; 1];

        let err = parse_resp_command(frame, &mut args).err().unwrap();
        assert!(matches!(err, RespParseError::InvalidArrayPrefix { .. }));
    }

    #[test]
    fn rejects_negative_bulk_length() {
        let frame = b"*1\r\n$-1\r\n";
        let mut args = [&b""[..]; 1];

        let err = parse_resp_command(frame, &mut args).err().unwrap();
        assert!(matches!(err, RespParseError::NegativeBulkLength { .. }));
    }

    #[test]
    fn returns_capacity_error_when_argument_slice_is_too_small() {
        let frame = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let mut args = [&b""[..]; 2];

        let err = parse_resp_command(frame, &mut args).err().unwrap();
        assert!(matches!(
            err,
            RespParseError::ArgumentCapacityExceeded { .. }
        ));
    }
}
