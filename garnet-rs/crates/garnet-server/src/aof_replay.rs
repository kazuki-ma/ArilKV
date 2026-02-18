//! AOF replay helpers for rebuilding in-memory state from command logs.

use crate::request_lifecycle::RequestProcessor;
use garnet_common::{parse_resp_command_arg_slices, ArgSlice};
use std::io;
use std::path::Path;
use tsavorite::AofReader;

pub fn replay_aof_file<P: AsRef<Path>>(processor: &RequestProcessor, path: P) -> io::Result<usize> {
    let mut reader = AofReader::open(path)?;
    let operations = reader.replay_all_tolerant()?;
    replay_aof_operations(processor, &operations)
}

pub fn replay_aof_operations(
    processor: &RequestProcessor,
    operations: &[Vec<u8>],
) -> io::Result<usize> {
    let mut args = [ArgSlice::EMPTY; 64];
    let mut response = Vec::new();
    let mut applied = 0usize;

    for operation in operations {
        response.clear();
        let meta = parse_resp_command_arg_slices(operation, &mut args).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid AOF operation frame: {:?}", error),
            )
        })?;
        if meta.bytes_consumed != operation.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "AOF operation frame contains trailing bytes",
            ));
        }

        processor
            .execute(&args[..meta.argument_count], &mut response)
            .map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("AOF operation failed during replay: {:?}", error),
                )
            })?;
        applied += 1;
    }

    Ok(applied)
}

#[cfg(test)]
mod tests {
    use super::*;
    use garnet_common::parse_resp_command_arg_slices;
    use tsavorite::{AofWriter, AofWriterConfig};

    fn temp_path(suffix: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("garnet-aof-replay-{}-{}.aof", suffix, nanos))
    }

    #[test]
    fn replay_aof_file_applies_operations_to_request_processor() {
        let path = temp_path("roundtrip");
        let mut writer = AofWriter::open(&path, AofWriterConfig { flush_every_ops: 1 }).unwrap();
        writer
            .append_operation(b"*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n")
            .unwrap();
        writer
            .append_operation(b"*2\r\n$3\r\nDEL\r\n$4\r\nkey1\r\n")
            .unwrap();
        writer
            .append_operation(b"*3\r\n$3\r\nSET\r\n$4\r\nkey2\r\n$6\r\nvalue2\r\n")
            .unwrap();
        writer.sync_all().unwrap();

        let processor = RequestProcessor::new().unwrap();
        let applied = replay_aof_file(&processor, &path).unwrap();
        assert_eq!(applied, 3);

        let mut args = [ArgSlice::EMPTY; 3];
        let mut response = Vec::new();

        let get_deleted = b"*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n";
        let meta = parse_resp_command_arg_slices(get_deleted, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$-1\r\n");

        response.clear();
        let get_existing = b"*2\r\n$3\r\nGET\r\n$4\r\nkey2\r\n";
        let meta = parse_resp_command_arg_slices(get_existing, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$6\r\nvalue2\r\n");

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn replay_returns_error_for_invalid_operation_frame() {
        let processor = RequestProcessor::new().unwrap();
        let operations = vec![b"not-a-resp-frame".to_vec()];
        let error = replay_aof_operations(&processor, &operations)
            .err()
            .unwrap();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("invalid AOF operation frame"));
    }
}
