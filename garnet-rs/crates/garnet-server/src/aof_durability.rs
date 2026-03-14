//! Live local AOF durability tracking runtime for `WAITAOF` prerequisites.

use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use tsavorite::AofOffset;
use tsavorite::AofWriter;
use tsavorite::AofWriterConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AppendFsyncPolicy {
    Always,
    Everysec,
    No,
}

impl AppendFsyncPolicy {
    pub(crate) fn parse(value: &[u8]) -> Option<Self> {
        if value.eq_ignore_ascii_case(b"always") {
            return Some(Self::Always);
        }
        if value.eq_ignore_ascii_case(b"everysec") {
            return Some(Self::Everysec);
        }
        if value.eq_ignore_ascii_case(b"no") {
            return Some(Self::No);
        }
        None
    }
}

#[derive(Debug, Clone)]
pub(crate) struct LiveAofDurabilityConfig {
    pub(crate) path: PathBuf,
    pub(crate) fsync_policy: AppendFsyncPolicy,
    pub(crate) flush_every_ops: usize,
    everysec_interval: Duration,
}

impl LiveAofDurabilityConfig {
    pub(crate) fn new(path: PathBuf, fsync_policy: AppendFsyncPolicy) -> Self {
        Self {
            path,
            fsync_policy,
            flush_every_ops: AofWriterConfig::default().flush_every_ops,
            everysec_interval: Duration::from_secs(1),
        }
    }

    #[cfg(test)]
    fn with_everysec_interval(mut self, everysec_interval: Duration) -> Self {
        self.everysec_interval = everysec_interval;
        self
    }
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LocalAofFrontiersSnapshot {
    pub(crate) append_offset: AofOffset,
    pub(crate) fsync_offset: AofOffset,
}

pub(crate) struct LiveAofDurabilityRuntime {
    publisher: mpsc::UnboundedSender<Arc<[u8]>>,
    local_append_offset: AtomicU64,
    local_fsync_offset: AtomicU64,
    frontier_notify: Notify,
}

impl LiveAofDurabilityRuntime {
    pub(crate) fn start(config: LiveAofDurabilityConfig) -> io::Result<Arc<Self>> {
        if let Some(parent) = config.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let writer = AofWriter::open(
            &config.path,
            AofWriterConfig {
                flush_every_ops: config.flush_every_ops.max(1),
            },
        )?;
        let (publisher, receiver) = mpsc::unbounded_channel();
        let runtime = Arc::new(Self {
            publisher,
            local_append_offset: AtomicU64::new(0),
            local_fsync_offset: AtomicU64::new(0),
            frontier_notify: Notify::new(),
        });
        let worker_runtime = Arc::clone(&runtime);
        tokio::spawn(async move {
            if let Err(error) =
                run_live_aof_writer_task(worker_runtime, config, writer, receiver).await
            {
                eprintln!("live aof durability runtime stopped after IO failure: {error}");
            }
        });
        Ok(runtime)
    }

    pub(crate) fn publish_frame(&self, frame: &[u8]) {
        let _ = self.publisher.send(Arc::from(frame.to_vec()));
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn snapshot(&self) -> LocalAofFrontiersSnapshot {
        LocalAofFrontiersSnapshot {
            append_offset: AofOffset::new(self.local_append_offset.load(Ordering::Acquire)),
            fsync_offset: AofOffset::new(self.local_fsync_offset.load(Ordering::Acquire)),
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn wait_for_frontiers_at_least(
        &self,
        target: LocalAofFrontiersSnapshot,
        timeout: Duration,
    ) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let snapshot = self.snapshot();
            if snapshot.append_offset >= target.append_offset
                && snapshot.fsync_offset >= target.fsync_offset
            {
                return true;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return false;
            }
            let notified = self.frontier_notify.notified();
            if tokio::time::timeout(remaining, notified).await.is_err() {
                return false;
            }
        }
    }

    fn publish_local_append_offset(&self, offset: AofOffset) {
        self.local_append_offset
            .fetch_max(u64::from(offset), Ordering::AcqRel);
        self.frontier_notify.notify_waiters();
    }

    fn publish_local_fsync_offset(&self, offset: AofOffset) {
        self.local_fsync_offset
            .fetch_max(u64::from(offset), Ordering::AcqRel);
        self.frontier_notify.notify_waiters();
    }
}

async fn run_live_aof_writer_task(
    runtime: Arc<LiveAofDurabilityRuntime>,
    config: LiveAofDurabilityConfig,
    mut writer: AofWriter,
    mut receiver: mpsc::UnboundedReceiver<Arc<[u8]>>,
) -> io::Result<()> {
    match config.fsync_policy {
        AppendFsyncPolicy::Always => {
            while let Some(frame) = receiver.recv().await {
                writer.append_operation(&frame)?;
                let append_offset = writer.current_offset()?;
                runtime.publish_local_append_offset(append_offset);
                writer.sync_all()?;
                let fsync_offset = writer.current_offset()?;
                runtime.publish_local_fsync_offset(fsync_offset);
            }
            Ok(())
        }
        AppendFsyncPolicy::No => {
            while let Some(frame) = receiver.recv().await {
                writer.append_operation(&frame)?;
                let append_offset = writer.current_offset()?;
                runtime.publish_local_append_offset(append_offset);
            }
            Ok(())
        }
        AppendFsyncPolicy::Everysec => {
            let mut pending_fsync_offset = None;
            let mut interval = tokio::time::interval(config.everysec_interval);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    maybe_frame = receiver.recv() => {
                        let Some(frame) = maybe_frame else {
                            break;
                        };
                        writer.append_operation(&frame)?;
                        let append_offset = writer.current_offset()?;
                        runtime.publish_local_append_offset(append_offset);
                        pending_fsync_offset = Some(append_offset);
                    }
                    _ = interval.tick() => {
                        let Some(target_fsync_offset) = pending_fsync_offset.take() else {
                            continue;
                        };
                        writer.sync_all()?;
                        let fsync_offset = writer.current_offset()?;
                        runtime.publish_local_fsync_offset(fsync_offset.max(target_fsync_offset));
                    }
                }
            }
            if let Some(target_fsync_offset) = pending_fsync_offset {
                writer.sync_all()?;
                let fsync_offset = writer.current_offset()?;
                runtime.publish_local_fsync_offset(fsync_offset.max(target_fsync_offset));
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;
    use tsavorite::AofReader;

    fn temp_aof_path(suffix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("garnet-live-aof-{suffix}-{nanos}.aof"))
    }

    #[tokio::test]
    async fn live_aof_runtime_appends_and_fsyncs_with_always_policy() {
        let path = temp_aof_path("always");
        let runtime = LiveAofDurabilityRuntime::start(LiveAofDurabilityConfig::new(
            path.clone(),
            AppendFsyncPolicy::Always,
        ))
        .unwrap();
        let frames = [
            b"*1\r\n$4\r\nPING\r\n".as_slice(),
            b"*1\r\n$4\r\nPONG\r\n".as_slice(),
        ];
        let expected_offset = u64::try_from((4 + frames[0].len()) + (4 + frames[1].len())).unwrap();
        for frame in &frames {
            runtime.publish_frame(frame);
        }
        let reached = runtime
            .wait_for_frontiers_at_least(
                LocalAofFrontiersSnapshot {
                    append_offset: AofOffset::new(expected_offset),
                    fsync_offset: AofOffset::new(expected_offset),
                },
                Duration::from_secs(1),
            )
            .await;
        assert!(reached, "timed out waiting for append+fsync frontier");
        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.append_offset, AofOffset::new(expected_offset));
        assert_eq!(snapshot.fsync_offset, AofOffset::new(expected_offset));
        let mut reader = AofReader::open(&path).unwrap();
        assert_eq!(
            reader.replay_all_tolerant().unwrap(),
            frames
                .iter()
                .map(|frame| frame.to_vec())
                .collect::<Vec<_>>()
        );
        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn live_aof_runtime_advances_append_without_fsync_when_policy_is_no() {
        let path = temp_aof_path("no");
        let runtime = LiveAofDurabilityRuntime::start(LiveAofDurabilityConfig::new(
            path.clone(),
            AppendFsyncPolicy::No,
        ))
        .unwrap();
        let frame = b"*1\r\n$4\r\nPING\r\n";
        let expected_offset = u64::try_from(4 + frame.len()).unwrap();
        runtime.publish_frame(frame);
        let reached = runtime
            .wait_for_frontiers_at_least(
                LocalAofFrontiersSnapshot {
                    append_offset: AofOffset::new(expected_offset),
                    fsync_offset: AofOffset::new(0),
                },
                Duration::from_secs(1),
            )
            .await;
        assert!(reached, "timed out waiting for append frontier");
        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.append_offset, AofOffset::new(expected_offset));
        assert_eq!(snapshot.fsync_offset, AofOffset::new(0));
        let mut reader = AofReader::open(&path).unwrap();
        assert_eq!(reader.replay_all_tolerant().unwrap(), vec![frame.to_vec()]);
        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn live_aof_runtime_everysec_policy_advances_fsync_frontier_on_tick() {
        let path = temp_aof_path("everysec");
        let runtime = LiveAofDurabilityRuntime::start(
            LiveAofDurabilityConfig::new(path.clone(), AppendFsyncPolicy::Everysec)
                .with_everysec_interval(Duration::from_millis(10)),
        )
        .unwrap();
        let frame = b"*1\r\n$4\r\nPING\r\n";
        let expected_offset = u64::try_from(4 + frame.len()).unwrap();
        runtime.publish_frame(frame);
        let reached = runtime
            .wait_for_frontiers_at_least(
                LocalAofFrontiersSnapshot {
                    append_offset: AofOffset::new(expected_offset),
                    fsync_offset: AofOffset::new(expected_offset),
                },
                Duration::from_secs(1),
            )
            .await;
        assert!(reached, "timed out waiting for everysec fsync frontier");
        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.append_offset, AofOffset::new(expected_offset));
        assert_eq!(snapshot.fsync_offset, AofOffset::new(expected_offset));
        let _ = fs::remove_file(path);
    }
}
