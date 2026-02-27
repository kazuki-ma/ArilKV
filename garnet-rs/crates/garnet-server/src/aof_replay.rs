//! AOF replay helpers for rebuilding in-memory state from command logs.

use crate::request_lifecycle::RequestProcessor;
use garnet_common::ArgSlice;
use garnet_common::parse_resp_command_arg_slices_dynamic;
use std::io;
use std::path::Path;
use tsavorite::AofReader;

const MAX_AOF_REPLAY_ARGUMENTS: usize = 1_048_576;

pub fn replay_aof_file<P: AsRef<Path>>(processor: &RequestProcessor, path: P) -> io::Result<usize> {
    let mut reader = AofReader::open(path)?;
    let operations = reader.replay_all_tolerant()?;
    replay_aof_operations(processor, &operations)
}

pub fn replay_aof_operations(
    processor: &RequestProcessor,
    operations: &[Vec<u8>],
) -> io::Result<usize> {
    let mut args = vec![ArgSlice::EMPTY; 64];
    let mut response = Vec::new();
    let mut applied = 0usize;

    for operation in operations {
        response.clear();
        let meta =
            parse_resp_command_arg_slices_dynamic(operation, &mut args, MAX_AOF_REPLAY_ARGUMENTS)
                .map_err(|error| {
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
    use sha1::Digest;
    use sha1::Sha1;
    use tsavorite::AofWriter;
    use tsavorite::AofWriterConfig;
    use tsavorite::CheckpointAofCoordinator;
    use tsavorite::CheckpointToken;
    use tsavorite::compact_aof_file;

    fn temp_path(suffix: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("garnet-aof-replay-{}-{}.aof", suffix, nanos))
    }

    fn encode_resp_frame(parts: &[&[u8]]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
        for part in parts {
            out.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
            out.extend_from_slice(part);
            out.extend_from_slice(b"\r\n");
        }
        out
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
    fn crash_recovery_replays_tail_after_checkpoint_compaction() {
        let source_aof = temp_path("crash-source");
        let compacted_aof = temp_path("crash-compacted");

        let checkpoint_frames = vec![
            b"*3\r\n$3\r\nSET\r\n$4\r\nkeya\r\n$4\r\nbase\r\n".to_vec(),
            b"*3\r\n$3\r\nSET\r\n$4\r\nkeyb\r\n$3\r\nold\r\n".to_vec(),
        ];
        let tail_frames = vec![
            b"*3\r\n$3\r\nSET\r\n$4\r\nkeyb\r\n$3\r\nnew\r\n".to_vec(),
            b"*2\r\n$3\r\nDEL\r\n$4\r\nkeya\r\n".to_vec(),
            b"*3\r\n$3\r\nSET\r\n$4\r\nkeyc\r\n$4\r\nlive\r\n".to_vec(),
        ];

        let mut writer =
            AofWriter::open(&source_aof, AofWriterConfig { flush_every_ops: 1 }).unwrap();
        let mut coordinator = CheckpointAofCoordinator::new();
        let checkpoint_token = CheckpointToken::new(77);
        let begin_offset = writer.current_offset().unwrap();
        coordinator
            .begin_checkpoint(checkpoint_token, begin_offset)
            .unwrap();

        for frame in &checkpoint_frames {
            writer.append_operation(frame).unwrap();
        }

        let checkpoint_tail_offset = writer.current_offset().unwrap();
        let plan = coordinator
            .complete_checkpoint(checkpoint_token, checkpoint_tail_offset)
            .unwrap();

        for frame in &tail_frames {
            writer.append_operation(frame).unwrap();
        }
        writer.sync_all().unwrap();

        let copied =
            compact_aof_file(&source_aof, &compacted_aof, plan.replay_from_aof_offset).unwrap();
        assert!(copied > 0);

        // Simulate checkpoint restore, then AOF tail replay on restart.
        let recovered_processor = RequestProcessor::new().unwrap();
        let checkpoint_applied =
            replay_aof_operations(&recovered_processor, &checkpoint_frames).unwrap();
        assert_eq!(checkpoint_applied, checkpoint_frames.len());
        let tail_applied = replay_aof_file(&recovered_processor, &compacted_aof).unwrap();
        assert_eq!(tail_applied, tail_frames.len());

        let mut args = [ArgSlice::EMPTY; 4];
        let mut response = Vec::new();

        let get_keya = b"*2\r\n$3\r\nGET\r\n$4\r\nkeya\r\n";
        let meta = parse_resp_command_arg_slices(get_keya, &mut args).unwrap();
        recovered_processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$-1\r\n");

        response.clear();
        let get_keyb = b"*2\r\n$3\r\nGET\r\n$4\r\nkeyb\r\n";
        let meta = parse_resp_command_arg_slices(get_keyb, &mut args).unwrap();
        recovered_processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$3\r\nnew\r\n");

        response.clear();
        let get_keyc = b"*2\r\n$3\r\nGET\r\n$4\r\nkeyc\r\n";
        let meta = parse_resp_command_arg_slices(get_keyc, &mut args).unwrap();
        recovered_processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$4\r\nlive\r\n");

        let _ = std::fs::remove_file(source_aof);
        let _ = std::fs::remove_file(compacted_aof);
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

    #[test]
    fn replay_aof_applies_script_load_and_evalsha_when_scripting_enabled() {
        let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true)
            .expect("processor initializes");
        let script = b"redis.call('SET', KEYS[1], ARGV[1]); return ARGV[1]";

        let mut hasher = Sha1::new();
        hasher.update(script);
        let script_sha = hasher.finalize();
        let sha_hex = script_sha
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();

        let operations = vec![
            encode_resp_frame(&[b"SCRIPT", b"LOAD", script]),
            encode_resp_frame(&[b"EVALSHA", sha_hex.as_bytes(), b"1", b"aof:lua:key", b"v1"]),
        ];
        let applied = replay_aof_operations(&processor, &operations).unwrap();
        assert_eq!(applied, 2);

        let mut args = [ArgSlice::EMPTY; 8];
        let mut response = Vec::new();
        let get_frame = encode_resp_frame(&[b"GET", b"aof:lua:key"]);
        let meta = parse_resp_command_arg_slices(&get_frame, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$2\r\nv1\r\n");
    }

    #[test]
    fn replay_aof_applies_function_load_and_fcall_when_scripting_enabled() {
        let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true)
            .expect("processor initializes");
        let library_source = b"#!lua name=lib_aof\nredis.register_function{function_name='rw_set', callback=function(keys, args) return redis.call('SET', keys[1], args[1]) end}";
        let operations = vec![
            encode_resp_frame(&[b"FUNCTION", b"LOAD", library_source]),
            encode_resp_frame(&[b"FCALL", b"rw_set", b"1", b"aof:function:key", b"v1"]),
        ];
        let applied = replay_aof_operations(&processor, &operations).unwrap();
        assert_eq!(applied, 2);

        let mut args = [ArgSlice::EMPTY; 8];
        let mut response = Vec::new();
        let get_frame = encode_resp_frame(&[b"GET", b"aof:function:key"]);
        let meta = parse_resp_command_arg_slices(&get_frame, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$2\r\nv1\r\n");
    }
}
