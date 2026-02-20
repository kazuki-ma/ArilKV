use std::io;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub(super) async fn write_resp_command(stream: &mut TcpStream, args: &[&[u8]]) -> io::Result<()> {
    let mut frame = Vec::with_capacity(256);
    frame.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for arg in args {
        frame.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        frame.extend_from_slice(arg);
        frame.extend_from_slice(b"\r\n");
    }
    stream.write_all(&frame).await
}

pub(super) async fn read_line(
    stream: &mut TcpStream,
    receive_buffer: &mut Vec<u8>,
    scratch: &mut [u8],
) -> io::Result<Vec<u8>> {
    loop {
        if let Some(position) = find_crlf(receive_buffer) {
            let mut line = receive_buffer.drain(..position + 2).collect::<Vec<u8>>();
            line.truncate(line.len().saturating_sub(2));
            return Ok(line);
        }

        let bytes_read = stream.read(scratch).await?;
        if bytes_read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "upstream closed stream while reading line",
            ));
        }
        receive_buffer.extend_from_slice(&scratch[..bytes_read]);
    }
}

pub(super) async fn discard_bulk_payload(
    stream: &mut TcpStream,
    receive_buffer: &mut Vec<u8>,
    scratch: &mut [u8],
    payload_len: usize,
) -> io::Result<()> {
    while receive_buffer.len() < payload_len {
        let bytes_read = stream.read(scratch).await?;
        if bytes_read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "upstream closed stream while sending RDB payload",
            ));
        }
        receive_buffer.extend_from_slice(&scratch[..bytes_read]);
    }
    receive_buffer.drain(..payload_len);

    // Redis FULLRESYNC bulk transfer framing is observed both with and without a
    // trailing CRLF before command streaming; tolerate either shape.
    if receive_buffer.starts_with(b"\r\n") {
        receive_buffer.drain(..2);
    }

    Ok(())
}

pub(super) fn parse_bulk_length(raw: &[u8]) -> io::Result<usize> {
    let value = std::str::from_utf8(raw)
        .ok()
        .and_then(|text| text.parse::<i64>().ok())
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, "invalid bulk length in replication")
        })?;
    if value < 0 {
        return Ok(0);
    }
    Ok(value as usize)
}

fn find_crlf(buffer: &[u8]) -> Option<usize> {
    buffer.windows(2).position(|window| window == b"\r\n")
}

pub(super) fn decode_hex_bytes(value: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(value.len() / 2);
    let bytes = value.as_bytes();
    let mut index = 0usize;
    while index + 1 < bytes.len() {
        let hi = decode_hex_nibble(bytes[index]);
        let lo = decode_hex_nibble(bytes[index + 1]);
        out.push((hi << 4) | lo);
        index += 2;
    }
    out
}

fn decode_hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        b'A'..=b'F' => byte - b'A' + 10,
        _ => 0,
    }
}

pub(super) fn starts_with_ascii_no_case(input: &[u8], expected_upper_prefix: &[u8]) -> bool {
    if input.len() < expected_upper_prefix.len() {
        return false;
    }

    input
        .iter()
        .take(expected_upper_prefix.len())
        .zip(expected_upper_prefix.iter())
        .all(|(lhs, rhs)| ascii_upper(*lhs) == *rhs)
}

fn ascii_upper(byte: u8) -> u8 {
    if byte.is_ascii_lowercase() {
        byte - 32
    } else {
        byte
    }
}

pub(super) fn generate_repl_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:040x}", nanos)
}
