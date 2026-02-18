//! Append-only file (AOF) primitives for durable operation logging.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;

const AOF_LENGTH_PREFIX_SIZE: usize = core::mem::size_of::<u32>();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AofWriterConfig {
    pub flush_every_ops: usize,
}

impl Default for AofWriterConfig {
    fn default() -> Self {
        Self {
            flush_every_ops: 128,
        }
    }
}

pub struct AofWriter {
    file: File,
    flush_every_ops: usize,
    pending_ops: usize,
}

impl AofWriter {
    pub fn open<P: AsRef<Path>>(path: P, config: AofWriterConfig) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;
        Ok(Self {
            file,
            flush_every_ops: config.flush_every_ops.max(1),
            pending_ops: 0,
        })
    }

    pub fn append_operation(&mut self, operation: &[u8]) -> io::Result<()> {
        let operation_len = u32::try_from(operation.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "operation payload exceeds u32 length prefix",
            )
        })?;
        self.file.write_all(&operation_len.to_le_bytes())?;
        self.file.write_all(operation)?;
        self.pending_ops += 1;
        if self.pending_ops >= self.flush_every_ops {
            self.flush()?;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()?;
        self.pending_ops = 0;
        Ok(())
    }

    pub fn sync_all(&mut self) -> io::Result<()> {
        self.flush()?;
        self.file.sync_all()
    }

    pub fn pending_ops(&self) -> usize {
        self.pending_ops
    }
}

pub struct AofReader {
    file: File,
}

impl AofReader {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        Ok(Self { file })
    }

    pub fn replay_all_tolerant(&mut self) -> io::Result<Vec<Vec<u8>>> {
        let mut operations = Vec::new();
        loop {
            match read_one_record(&mut self.file)? {
                Some(operation) => operations.push(operation),
                None => break,
            }
        }
        Ok(operations)
    }
}

fn read_one_record(file: &mut File) -> io::Result<Option<Vec<u8>>> {
    let mut len_raw = [0u8; AOF_LENGTH_PREFIX_SIZE];
    let read = file.read(&mut len_raw)?;
    if read == 0 {
        return Ok(None);
    }
    if read < AOF_LENGTH_PREFIX_SIZE {
        return Ok(None);
    }

    let operation_len = u32::from_le_bytes(len_raw) as usize;
    let mut operation = vec![0u8; operation_len];
    let mut read_total = 0usize;
    while read_total < operation_len {
        let n = file.read(&mut operation[read_total..])?;
        if n == 0 {
            return Ok(None);
        }
        read_total += n;
    }
    Ok(Some(operation))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_path(suffix: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("garnet-aof-{}-{}.aof", suffix, nanos))
    }

    #[test]
    fn aof_writer_and_reader_roundtrip_operations() {
        let path = temp_path("roundtrip");
        let mut writer = AofWriter::open(&path, AofWriterConfig { flush_every_ops: 2 }).unwrap();
        writer.append_operation(b"SET key value").unwrap();
        writer.append_operation(b"DEL key").unwrap();
        writer.sync_all().unwrap();

        let mut reader = AofReader::open(&path).unwrap();
        let operations = reader.replay_all_tolerant().unwrap();
        assert_eq!(
            operations,
            vec![b"SET key value".to_vec(), b"DEL key".to_vec()]
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn flush_policy_resets_pending_counter() {
        let path = temp_path("flush-policy");
        let mut writer = AofWriter::open(&path, AofWriterConfig { flush_every_ops: 2 }).unwrap();

        writer.append_operation(b"op1").unwrap();
        assert_eq!(writer.pending_ops(), 1);
        writer.append_operation(b"op2").unwrap();
        assert_eq!(writer.pending_ops(), 0);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn tolerant_replay_ignores_truncated_tail_record() {
        let path = temp_path("truncated-tail");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(3u32).to_le_bytes());
        bytes.extend_from_slice(b"SET");
        bytes.extend_from_slice(&(5u32).to_le_bytes());
        bytes.extend_from_slice(b"DEL"); // tail record intentionally truncated.
        fs::write(&path, &bytes).unwrap();

        let mut reader = AofReader::open(&path).unwrap();
        let operations = reader.replay_all_tolerant().unwrap();
        assert_eq!(operations, vec![b"SET".to_vec()]);

        let _ = fs::remove_file(path);
    }
}
