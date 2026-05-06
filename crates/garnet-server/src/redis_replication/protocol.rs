use std::io;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
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

#[allow(dead_code)]
pub(super) async fn discard_bulk_payload(
    stream: &mut TcpStream,
    receive_buffer: &mut Vec<u8>,
    scratch: &mut [u8],
    payload_len: usize,
) -> io::Result<()> {
    let _ = read_bulk_payload(stream, receive_buffer, scratch, payload_len).await?;
    Ok(())
}

pub(super) async fn read_bulk_payload(
    stream: &mut TcpStream,
    receive_buffer: &mut Vec<u8>,
    scratch: &mut [u8],
    payload_len: usize,
) -> io::Result<Vec<u8>> {
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
    let payload = receive_buffer.drain(..payload_len).collect::<Vec<u8>>();

    // Redis FULLRESYNC bulk transfer framing is observed both with and without a
    // trailing CRLF before command streaming; tolerate either shape.
    if receive_buffer.starts_with(b"\r\n") {
        receive_buffer.drain(..2);
    }

    Ok(payload)
}

pub(super) fn parse_bulk_length(raw: &[u8]) -> io::Result<usize> {
    let value = std::str::from_utf8(raw)
        .ok()
        .and_then(|text| text.parse::<i64>().ok())
        .ok_or_else(|| io::Error::other("invalid bulk length in replication"))?;
    if value < 0 {
        return Ok(0);
    }
    Ok(value as usize)
}

fn find_crlf(buffer: &[u8]) -> Option<usize> {
    buffer.windows(2).position(|window| window == b"\r\n")
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
