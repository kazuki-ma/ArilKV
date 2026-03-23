//! Live local AOF durability tracking runtime for `WAITAOF` prerequisites.

use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use tsavorite::AofOffset;
use tsavorite::AofWriter;
use tsavorite::AofWriterConfig;

const AOF_LENGTH_PREFIX_SIZE: u64 = core::mem::size_of::<u32>() as u64;

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

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LocalAofPublishTarget {
    pub(crate) append_offset: AofOffset,
}

struct PublishedAofFrame {
    frame: Arc<[u8]>,
    append_offset: AofOffset,
}

pub(crate) struct LiveAofDurabilityRuntime {
    publisher: Mutex<Option<mpsc::UnboundedSender<PublishedAofFrame>>>,
    next_append_target: AtomicU64,
    local_append_offset: AtomicU64,
    local_fsync_offset: AtomicU64,
    frontier_notify: Notify,
    shutdown_complete: Mutex<bool>,
    shutdown_complete_condvar: Condvar,
}

impl LiveAofDurabilityRuntime {
    pub(crate) fn start(config: LiveAofDurabilityConfig) -> io::Result<Arc<Self>> {
        if let Some(parent) = config.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut writer = AofWriter::open(
            &config.path,
            AofWriterConfig {
                flush_every_ops: config.flush_every_ops.max(1),
            },
        )?;
        let initial_offset = writer.current_offset()?;
        let (publisher, receiver) = mpsc::unbounded_channel();
        let runtime = Arc::new(Self {
            publisher: Mutex::new(Some(publisher)),
            next_append_target: AtomicU64::new(u64::from(initial_offset)),
            local_append_offset: AtomicU64::new(u64::from(initial_offset)),
            local_fsync_offset: AtomicU64::new(u64::from(initial_offset)),
            frontier_notify: Notify::new(),
            shutdown_complete: Mutex::new(false),
            shutdown_complete_condvar: Condvar::new(),
        });
        let worker_runtime = Arc::clone(&runtime);
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let result =
                    run_live_aof_writer_task(Arc::clone(&worker_runtime), config, writer, receiver)
                        .await;
                worker_runtime.mark_shutdown_complete();
                if let Err(error) = result {
                    eprintln!("live aof durability runtime stopped after IO failure: {error}");
                }
            });
        } else {
            std::thread::Builder::new()
                .name("garnet-live-aof".to_string())
                .spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build();
                    let Ok(runtime) = runtime else {
                        eprintln!("live aof durability runtime failed to create tokio runtime");
                        return;
                    };
                    runtime.block_on(async move {
                        let result = run_live_aof_writer_task(
                            Arc::clone(&worker_runtime),
                            config,
                            writer,
                            receiver,
                        )
                        .await;
                        worker_runtime.mark_shutdown_complete();
                        if let Err(error) = result {
                            eprintln!(
                                "live aof durability runtime stopped after IO failure: {error}"
                            );
                        }
                    });
                })
                .map_err(|error| {
                    io::Error::other(format!("failed to spawn live aof thread: {error}"))
                })?;
        }
        Ok(runtime)
    }

    pub(crate) fn publish_frame(&self, frame: &[u8]) -> LocalAofPublishTarget {
        let publisher = self
            .publisher
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .cloned();
        let record_len = AOF_LENGTH_PREFIX_SIZE.saturating_add(frame.len() as u64);
        let previous_tail = self
            .next_append_target
            .fetch_add(record_len, Ordering::AcqRel);
        let append_offset = AofOffset::new(previous_tail.saturating_add(record_len));
        if let Some(publisher) = publisher {
            let _ = publisher.send(PublishedAofFrame {
                frame: Arc::from(frame.to_vec()),
                append_offset,
            });
        }
        LocalAofPublishTarget { append_offset }
    }

    pub(crate) fn append_target_offset(&self) -> AofOffset {
        AofOffset::new(self.next_append_target.load(Ordering::Acquire))
    }

    pub(crate) fn shutdown(&self) {
        self.publisher
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        self.frontier_notify.notify_waiters();
    }

    pub(crate) fn shutdown_and_wait(&self, timeout: Duration) -> bool {
        self.shutdown();
        let deadline = Instant::now() + timeout;
        let mut complete = self
            .shutdown_complete
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while !*complete {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            match self
                .shutdown_complete_condvar
                .wait_timeout(complete, remaining)
            {
                Ok((guard, wait_result)) => {
                    complete = guard;
                    if wait_result.timed_out() && !*complete {
                        return false;
                    }
                }
                Err(poisoned) => {
                    let (guard, _) = poisoned.into_inner();
                    complete = guard;
                }
            }
        }
        true
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

    pub(crate) async fn wait_for_fsync_offset_at_least(
        &self,
        target_offset: AofOffset,
        timeout: Option<Duration>,
    ) -> bool {
        if self.snapshot().fsync_offset >= target_offset {
            return true;
        }
        let deadline = timeout.map(|timeout| tokio::time::Instant::now() + timeout);
        loop {
            let notified = self.frontier_notify.notified();
            if let Some(deadline) = deadline {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    return self.snapshot().fsync_offset >= target_offset;
                }
                if tokio::time::timeout(remaining, notified).await.is_err() {
                    return self.snapshot().fsync_offset >= target_offset;
                }
            } else {
                notified.await;
            }

            if self.snapshot().fsync_offset >= target_offset {
                return true;
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

    fn mark_shutdown_complete(&self) {
        let mut complete = self
            .shutdown_complete
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *complete = true;
        self.shutdown_complete_condvar.notify_all();
        self.frontier_notify.notify_waiters();
    }
}

async fn run_live_aof_writer_task(
    runtime: Arc<LiveAofDurabilityRuntime>,
    config: LiveAofDurabilityConfig,
    mut writer: AofWriter,
    mut receiver: mpsc::UnboundedReceiver<PublishedAofFrame>,
) -> io::Result<()> {
    match config.fsync_policy {
        AppendFsyncPolicy::Always => {
            while let Some(record) = receiver.recv().await {
                writer.append_operation(&record.frame)?;
                let append_offset = writer.current_offset()?;
                debug_assert_eq!(append_offset, record.append_offset);
                runtime.publish_local_append_offset(append_offset);
                writer.sync_all()?;
                let fsync_offset = writer.current_offset()?;
                runtime.publish_local_fsync_offset(fsync_offset);
            }
            Ok(())
        }
        AppendFsyncPolicy::No => {
            while let Some(record) = receiver.recv().await {
                writer.append_operation(&record.frame)?;
                let append_offset = writer.current_offset()?;
                debug_assert_eq!(append_offset, record.append_offset);
                runtime.publish_local_append_offset(append_offset);
            }
            Ok(())
        }
        AppendFsyncPolicy::Everysec => {
            let mut pending_fsync_offset = None;
            let mut interval = tokio::time::interval_at(
                tokio::time::Instant::now() + config.everysec_interval,
                config.everysec_interval,
            );
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    maybe_record = receiver.recv() => {
                        let Some(record) = maybe_record else {
                            break;
                        };
                        writer.append_operation(&record.frame)?;
                        let append_offset = writer.current_offset()?;
                        debug_assert_eq!(append_offset, record.append_offset);
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
        let publish_targets = frames
            .iter()
            .map(|frame| runtime.publish_frame(frame))
            .collect::<Vec<_>>();
        assert_eq!(
            publish_targets,
            vec![
                LocalAofPublishTarget {
                    append_offset: AofOffset::new(u64::try_from(4 + frames[0].len()).unwrap()),
                },
                LocalAofPublishTarget {
                    append_offset: AofOffset::new(expected_offset),
                },
            ]
        );
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
        let publish_target = runtime.publish_frame(frame);
        assert_eq!(
            publish_target,
            LocalAofPublishTarget {
                append_offset: AofOffset::new(expected_offset),
            }
        );
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
        let publish_target = runtime.publish_frame(frame);
        assert_eq!(
            publish_target,
            LocalAofPublishTarget {
                append_offset: AofOffset::new(expected_offset),
            }
        );
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

    #[tokio::test]
    async fn live_aof_runtime_everysec_policy_does_not_fsync_before_first_tick() {
        let path = temp_aof_path("everysec-no-early-fsync");
        let runtime = LiveAofDurabilityRuntime::start(
            LiveAofDurabilityConfig::new(path.clone(), AppendFsyncPolicy::Everysec)
                .with_everysec_interval(Duration::from_millis(100)),
        )
        .unwrap();
        let frame = b"*1\r\n$4\r\nPING\r\n";
        let expected_offset = u64::try_from(4 + frame.len()).unwrap();
        let publish_target = runtime.publish_frame(frame);
        assert_eq!(
            publish_target,
            LocalAofPublishTarget {
                append_offset: AofOffset::new(expected_offset),
            }
        );

        let reached_early = runtime
            .wait_for_fsync_offset_at_least(
                publish_target.append_offset,
                Some(Duration::from_millis(10)),
            )
            .await;
        assert!(
            !reached_early,
            "everysec policy should not fsync before the first interval elapses"
        );

        let reached_late = runtime
            .wait_for_fsync_offset_at_least(
                publish_target.append_offset,
                Some(Duration::from_secs(1)),
            )
            .await;
        assert!(
            reached_late,
            "everysec policy never advanced fsync frontier"
        );

        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn live_aof_runtime_bootstraps_frontiers_from_existing_aof_tail() {
        let path = temp_aof_path("bootstrap");
        let mut writer = AofWriter::open(&path, AofWriterConfig { flush_every_ops: 1 }).unwrap();
        writer.append_operation(b"*1\r\n$4\r\nPING\r\n").unwrap();
        writer.sync_all().unwrap();
        let existing_offset = writer.current_offset().unwrap();
        drop(writer);

        let runtime = LiveAofDurabilityRuntime::start(LiveAofDurabilityConfig::new(
            path.clone(),
            AppendFsyncPolicy::Always,
        ))
        .unwrap();
        assert_eq!(
            runtime.snapshot(),
            LocalAofFrontiersSnapshot {
                append_offset: existing_offset,
                fsync_offset: existing_offset,
            }
        );

        let frame = b"*1\r\n$4\r\nPONG\r\n";
        let publish_target = runtime.publish_frame(frame);
        assert_eq!(
            publish_target,
            LocalAofPublishTarget {
                append_offset: AofOffset::new(
                    u64::from(existing_offset)
                        .saturating_add(AOF_LENGTH_PREFIX_SIZE)
                        .saturating_add(frame.len() as u64),
                ),
            }
        );

        let reached = runtime
            .wait_for_frontiers_at_least(
                LocalAofFrontiersSnapshot {
                    append_offset: publish_target.append_offset,
                    fsync_offset: publish_target.append_offset,
                },
                Duration::from_secs(1),
            )
            .await;
        assert!(
            reached,
            "timed out waiting for bootstrapped frontier advance"
        );
        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn live_aof_runtime_waits_for_fsync_offset_target() {
        let path = temp_aof_path("wait-fsync");
        let runtime = LiveAofDurabilityRuntime::start(
            LiveAofDurabilityConfig::new(path.clone(), AppendFsyncPolicy::Everysec)
                .with_everysec_interval(Duration::from_millis(10)),
        )
        .unwrap();

        let frame = b"*1\r\n$4\r\nPING\r\n";
        let publish_target = runtime.publish_frame(frame);
        let reached = runtime
            .wait_for_fsync_offset_at_least(
                publish_target.append_offset,
                Some(Duration::from_secs(1)),
            )
            .await;
        assert!(reached, "timed out waiting for fsync frontier target");
        let snapshot = runtime.snapshot();
        assert!(snapshot.fsync_offset >= publish_target.append_offset);

        let _ = fs::remove_file(path);
    }
}
