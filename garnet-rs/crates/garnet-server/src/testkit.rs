use crate::RequestExecutionError;
use crate::RequestProcessor;
use crate::request_lifecycle::DbName;
use garnet_common::RespParseError;
use garnet_common::parse_resp_command_arg_slices_dynamic;

#[derive(Debug)]
pub(crate) enum CommandLineParseError {
    Empty,
    UnterminatedQuote(char),
    TrailingEscape,
}

impl core::fmt::Display for CommandLineParseError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Empty => write!(f, "command line is empty"),
            Self::UnterminatedQuote(quote) => {
                write!(f, "unterminated quote {}", quote)
            }
            Self::TrailingEscape => write!(f, "command line ends with trailing escape"),
        }
    }
}

impl std::error::Error for CommandLineParseError {}

#[derive(Debug)]
pub(crate) enum CommandHarnessError {
    Parse(CommandLineParseError),
    Protocol(RespParseError),
    TrailingBytes,
    Request(RequestExecutionError),
}

impl core::fmt::Display for CommandHarnessError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Parse(error) => error.fmt(f),
            Self::Protocol(error) => error.fmt(f),
            Self::TrailingBytes => write!(f, "RESP frame has trailing bytes"),
            Self::Request(error) => write!(f, "{error:?}"),
        }
    }
}

impl std::error::Error for CommandHarnessError {}

impl From<CommandLineParseError> for CommandHarnessError {
    fn from(value: CommandLineParseError) -> Self {
        Self::Parse(value)
    }
}

impl From<RespParseError> for CommandHarnessError {
    fn from(value: RespParseError) -> Self {
        Self::Protocol(value)
    }
}

impl From<RequestExecutionError> for CommandHarnessError {
    fn from(value: RequestExecutionError) -> Self {
        Self::Request(value)
    }
}

pub(crate) fn tokenize_command_line(line: &str) -> Result<Vec<Vec<u8>>, CommandLineParseError> {
    let mut tokens: Vec<Vec<u8>> = Vec::new();
    let mut current = Vec::new();
    let mut quote: Option<u8> = None;
    let mut escaping = false;

    for &byte in line.as_bytes() {
        if escaping {
            current.push(byte);
            escaping = false;
            continue;
        }

        if byte == b'\\' {
            escaping = true;
            continue;
        }

        if let Some(expected_quote) = quote {
            if byte == expected_quote {
                quote = None;
            } else {
                current.push(byte);
            }
            continue;
        }

        if byte == b'\'' || byte == b'"' {
            quote = Some(byte);
            continue;
        }

        if byte.is_ascii_whitespace() {
            if !current.is_empty() {
                tokens.push(core::mem::take(&mut current));
            }
            continue;
        }

        current.push(byte);
    }

    if escaping {
        return Err(CommandLineParseError::TrailingEscape);
    }
    if let Some(unclosed) = quote {
        return Err(CommandLineParseError::UnterminatedQuote(unclosed as char));
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    if tokens.is_empty() {
        return Err(CommandLineParseError::Empty);
    }
    Ok(tokens)
}

pub(crate) fn encode_resp_command(parts: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
    for part in parts {
        out.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        out.extend_from_slice(part);
        out.extend_from_slice(b"\r\n");
    }
    out
}

pub(crate) fn execute_resp_frame(
    processor: &RequestProcessor,
    frame: &[u8],
) -> Result<Vec<u8>, CommandHarnessError> {
    execute_resp_frame_in_db(processor, frame, DbName::default())
}

pub(crate) fn execute_resp_frame_in_db(
    processor: &RequestProcessor,
    frame: &[u8],
    selected_db: DbName,
) -> Result<Vec<u8>, CommandHarnessError> {
    let mut args = Vec::new();
    let meta = parse_resp_command_arg_slices_dynamic(frame, &mut args, usize::MAX)?;
    if meta.bytes_consumed != frame.len() {
        return Err(CommandHarnessError::TrailingBytes);
    }

    let mut response = Vec::new();
    processor
        .execute_in_db(&args[..meta.argument_count], &mut response, selected_db)
        .map_err(CommandHarnessError::Request)?;
    Ok(response)
}

pub(crate) fn execute_command_line(
    processor: &RequestProcessor,
    line: &str,
) -> Result<Vec<u8>, CommandHarnessError> {
    let tokens = tokenize_command_line(line)?;
    let frame = encode_resp_command(&tokens);
    execute_resp_frame(processor, &frame)
}

pub(crate) fn execute_command_line_in_db(
    processor: &RequestProcessor,
    line: &str,
    selected_db: DbName,
) -> Result<Vec<u8>, CommandHarnessError> {
    let tokens = tokenize_command_line(line)?;
    let frame = encode_resp_command(&tokens);
    execute_resp_frame_in_db(processor, &frame, selected_db)
}

#[cfg(test)]
pub(crate) fn assert_command_response(processor: &RequestProcessor, line: &str, expected: &[u8]) {
    assert_command_response_in_db(processor, line, expected, DbName::default());
}

#[cfg(test)]
pub(crate) fn assert_command_response_in_db(
    processor: &RequestProcessor,
    line: &str,
    expected: &[u8],
    selected_db: DbName,
) {
    let response = execute_command_line_in_db(processor, line, selected_db)
        .unwrap_or_else(|error| panic!("command failed: `{line}`: {error}"));
    assert_eq!(response, expected, "unexpected response for `{line}`");
}

#[cfg(test)]
pub(crate) fn assert_command_integer(processor: &RequestProcessor, line: &str, expected: i64) {
    let response = execute_command_line(processor, line)
        .unwrap_or_else(|error| panic!("command failed: `{line}`: {error}"));
    assert!(
        response.len() >= 4 && response[0] == b':' && response.ends_with(b"\r\n"),
        "expected integer RESP reply for `{line}`, got: {:?}",
        String::from_utf8_lossy(&response)
    );
    let value = core::str::from_utf8(&response[1..response.len() - 2])
        .unwrap()
        .parse::<i64>()
        .unwrap();
    assert_eq!(value, expected, "unexpected integer response for `{line}`");
}

#[cfg(test)]
pub(crate) fn assert_command_error(processor: &RequestProcessor, line: &str, expected: &[u8]) {
    match execute_command_line(processor, line) {
        Ok(response) => panic!(
            "command unexpectedly succeeded: `{line}` -> {:?}",
            String::from_utf8_lossy(&response)
        ),
        Err(CommandHarnessError::Request(error)) => {
            let mut response = Vec::new();
            error.append_resp_error(&mut response);
            assert_eq!(response, expected, "unexpected error response for `{line}`");
        }
        Err(error) => panic!("command failed with non-request error for `{line}`: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_command_line_supports_quotes_and_escapes() {
        let tokens = tokenize_command_line("SET \"key with space\" 'v\\'1'").unwrap();
        assert_eq!(
            tokens,
            vec![b"SET".to_vec(), b"key with space".to_vec(), b"v'1".to_vec()]
        );
    }

    #[test]
    fn execute_command_line_roundtrips_set_and_get() {
        let processor = RequestProcessor::new().unwrap();
        assert_eq!(
            execute_command_line(&processor, "SET k \"hello world\"").unwrap(),
            b"+OK\r\n"
        );
        assert_eq!(
            execute_command_line(&processor, "GET k").unwrap(),
            b"$11\r\nhello world\r\n"
        );
    }

    #[test]
    fn execute_command_line_reports_parse_error() {
        let processor = RequestProcessor::new().unwrap();
        let error = execute_command_line(&processor, "SET \"broken")
            .expect_err("unterminated quote must fail");
        assert!(matches!(
            error,
            CommandHarnessError::Parse(CommandLineParseError::UnterminatedQuote('"'))
        ));
    }
}
