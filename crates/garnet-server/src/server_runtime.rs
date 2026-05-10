// TLA+ model: formal/tla/specs/ScriptActiveExpireFreeze.tla

use std::collections::HashSet;
use std::future::Future;
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use garnet_cluster::ClusterConfigStore;
use tokio::net::TcpListener;
use tokio::task::JoinSet;

use crate::RequestProcessor;
use crate::ServerConfig;
use crate::ServerMetrics;
use crate::StartupConfigOverrides;
use crate::build_owner_thread_pool;
use crate::config_paths::normalize_config_dir_path;
use crate::handle_connection;
use crate::redis_replication::RedisReplicationCoordinator;
use crate::request_lifecycle::ShardIndex;

#[cfg(test)]
pub(crate) fn isolate_default_persistence_dir_for_tests(
    processor: &RequestProcessor,
) -> Option<std::path::PathBuf> {
    let default_dir = std::env::current_dir()
        .unwrap_or_else(|_| std::path::PathBuf::from("."))
        .to_string_lossy()
        .into_owned()
        .into_bytes();
    let configured_dir = processor
        .config_items_snapshot()
        .into_iter()
        .find(|(key, _)| key == b"dir")
        .map(|(_, value)| value)
        .unwrap_or_else(|| default_dir.clone());
    if configured_dir != default_dir {
        return None;
    }

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("garnet-server-test-persistence-{nanos}"));
    let _ = std::fs::create_dir_all(&dir);
    processor.set_config_value(b"dir", dir.to_string_lossy().into_owned().into_bytes());
    Some(dir)
}

#[cfg(not(test))]
pub(crate) fn isolate_default_persistence_dir_for_tests(
    _processor: &RequestProcessor,
) -> Option<std::path::PathBuf> {
    None
}

pub(crate) fn apply_startup_config_overrides_to_processor(
    processor: &RequestProcessor,
    overrides: &StartupConfigOverrides,
) -> io::Result<()> {
    if let Some(dir) = overrides.dir.as_ref() {
        processor.set_config_value(b"dir", normalize_config_dir_path(dir));
    }
    if let Some(dbfilename) = overrides.dbfilename.as_ref() {
        processor.set_config_value(b"dbfilename", dbfilename.clone().into_bytes());
    }
    if let Some(appendonly) = overrides.appendonly {
        let value: &[u8] = if appendonly { b"yes" } else { b"no" };
        processor.set_config_value(b"appendonly", value.to_vec());
    }
    if let Some(appendfsync) = overrides.appendfsync.as_ref() {
        processor.set_config_value(b"appendfsync", appendfsync.clone().into_bytes());
    }
    if let Some(appendfilename) = overrides.appendfilename.as_ref() {
        processor.set_config_value(b"appendfilename", appendfilename.clone().into_bytes());
    }
    if let Some(aclfile) = overrides.aclfile.as_ref() {
        processor.set_config_value(
            b"aclfile",
            aclfile
                .as_os_str()
                .to_string_lossy()
                .into_owned()
                .into_bytes(),
        );
    }
    if overrides.aclfile.is_some() {
        processor
            .load_acl_users_from_configured_file()
            .map_err(io::Error::other)?;
    }
    if !overrides.users.is_empty() {
        let mut seen_users = HashSet::<Vec<u8>>::new();
        for definition in &overrides.users {
            let tokens = definition
                .split_ascii_whitespace()
                .map(str::as_bytes)
                .collect::<Vec<_>>();
            let Some(username) = tokens.first().copied() else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "ERR Invalid empty ACL user definition",
                ));
            };
            if !seen_users.insert(username.to_vec()) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "ERR Duplicate user '{}' found in startup config",
                        String::from_utf8_lossy(username)
                    ),
                ));
            }
            processor
                .set_acl_user_from_startup_definition(definition)
                .map_err(io::Error::other)?;
        }
    }
    Ok(())
}

pub async fn run(config: ServerConfig, metrics: Arc<ServerMetrics>) -> io::Result<()> {
    run_with_shutdown(config, metrics, std::future::pending::<()>()).await
}

pub async fn run_with_cluster(
    config: ServerConfig,
    metrics: Arc<ServerMetrics>,
    cluster_config: Arc<ClusterConfigStore>,
) -> io::Result<()> {
    run_with_shutdown_and_cluster_config(
        config,
        metrics,
        std::future::pending::<()>(),
        Some(cluster_config),
    )
    .await
}

pub async fn run_with_shutdown<F>(
    config: ServerConfig,
    metrics: Arc<ServerMetrics>,
    shutdown: F,
) -> io::Result<()>
where
    F: Future<Output = ()> + Send,
{
    run_with_shutdown_and_cluster_config(config, metrics, shutdown, None).await
}

pub async fn run_with_shutdown_and_cluster_config<F>(
    config: ServerConfig,
    metrics: Arc<ServerMetrics>,
    shutdown: F,
    cluster_config: Option<Arc<ClusterConfigStore>>,
) -> io::Result<()>
where
    F: Future<Output = ()> + Send,
{
    let listener = TcpListener::bind(config.bind_addr).await?;
    let processor = Arc::new(RequestProcessor::new().map_err(|err| {
        io::Error::other(format!("request processor initialization failed: {err}"))
    })?);
    apply_startup_config_overrides_to_processor(
        processor.as_ref(),
        &config.startup_config_overrides,
    )?;
    run_listener_with_shutdown_and_cluster_with_processor(
        listener,
        config.read_buffer_size,
        metrics,
        shutdown,
        cluster_config,
        processor,
    )
    .await
}

pub async fn run_listener_with_shutdown<F>(
    listener: TcpListener,
    read_buffer_size: usize,
    metrics: Arc<ServerMetrics>,
    shutdown: F,
) -> io::Result<()>
where
    F: Future<Output = ()> + Send,
{
    run_listener_with_shutdown_and_cluster(listener, read_buffer_size, metrics, shutdown, None)
        .await
}

pub async fn run_listener_with_shutdown_and_cluster<F>(
    listener: TcpListener,
    read_buffer_size: usize,
    metrics: Arc<ServerMetrics>,
    shutdown: F,
    cluster_config: Option<Arc<ClusterConfigStore>>,
) -> io::Result<()>
where
    F: Future<Output = ()> + Send,
{
    let processor = Arc::new(RequestProcessor::new().map_err(|err| {
        io::Error::other(format!("request processor initialization failed: {err}"))
    })?);
    run_listener_with_shutdown_and_cluster_with_processor(
        listener,
        read_buffer_size,
        metrics,
        shutdown,
        cluster_config,
        processor,
    )
    .await
}

pub async fn run_listener_with_shutdown_and_cluster_with_processor<F>(
    listener: TcpListener,
    read_buffer_size: usize,
    metrics: Arc<ServerMetrics>,
    shutdown: F,
    cluster_config: Option<Arc<ClusterConfigStore>>,
    processor: Arc<RequestProcessor>,
) -> io::Result<()>
where
    F: Future<Output = ()> + Send,
{
    let _ = isolate_default_persistence_dir_for_tests(processor.as_ref());
    if let Ok(local_addr) = listener.local_addr() {
        processor.set_config_value(b"bind", local_addr.ip().to_string().into_bytes());
        processor.set_config_value(b"port", local_addr.port().to_string().into_bytes());
    }
    if let Some(cluster_config_store) = cluster_config.as_ref() {
        processor.attach_cluster_config_store(Arc::clone(cluster_config_store));
    }
    if processor.appendonly_enabled() {
        if processor.configured_appendonly_source_exists() {
            processor.reload_configured_aof_source().map_err(|error| {
                io::Error::other(format!(
                    "configured appendonly replay failed during startup: {error:?}"
                ))
            })?;
        } else {
            processor
                .reload_configured_dump_snapshot_file()
                .map_err(|error| {
                    io::Error::other(format!(
                        "configured dump snapshot replay failed during startup: {error:?}"
                    ))
                })?;
        }
    } else {
        processor
            .reload_configured_dump_snapshot_file()
            .map_err(|error| {
                io::Error::other(format!(
                    "configured dump snapshot replay failed during startup: {error:?}"
                ))
            })?;
    }
    processor
        .ensure_live_aof_durability_runtime()
        .map_err(|error| {
            io::Error::other(format!(
                "live aof durability runtime initialization failed: {error}"
            ))
        })?;

    let mut tasks = JoinSet::new();
    let owner_thread_pool = build_owner_thread_pool(&processor)?;
    let replication = Arc::new(RedisReplicationCoordinator::new(
        Arc::clone(&processor),
        Arc::clone(&owner_thread_pool),
    ));
    if let Ok(local_addr) = listener.local_addr() {
        replication.set_listen_port(local_addr.port());
    }
    processor.attach_replication_coordinator(Arc::clone(&replication));
    let expiration_processor = Arc::clone(&processor);
    let expiration_shard_count = processor.string_store_shard_count();
    let expiration_owner_thread_pool = Arc::clone(&owner_thread_pool);
    let expiration_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(50));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            // TLA+ : SkipActiveExpireWhileScriptRunning
            if !expiration_processor.active_expire_enabled()
                || expiration_processor.is_expire_action_paused()
                || expiration_processor.debug_pause_cron()
                || expiration_processor.script_execution_in_progress()
            {
                continue;
            }
            for shard in 0..expiration_shard_count {
                let shard_index = ShardIndex::new(shard);
                let routed_processor = Arc::clone(&expiration_processor);
                let _ = expiration_owner_thread_pool.execute_sync(shard_index, move || {
                    let _ = routed_processor.expire_stale_keys_in_shard(shard_index, 128);
                    let _ = routed_processor.expire_stale_auxiliary_keys_in_shard(shard_index, 128);
                    routed_processor.active_expire_hash_fields_in_shard(shard_index, 128)
                });
            }
        }
    });
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                break;
            }
            accept_result = listener.accept() => {
                let (stream, _) = accept_result?;
                let _ = stream.set_nodelay(true);
                metrics.accepted_connections.fetch_add(1, Ordering::Relaxed);
                metrics.active_connections.fetch_add(1, Ordering::Relaxed);

                let task_metrics = Arc::clone(&metrics);
                let task_processor = Arc::clone(&processor);
                let task_cluster = cluster_config.clone();
                let task_owner_threads = owner_thread_pool.clone();
                let task_replication = Arc::clone(&replication);
                tasks.spawn(async move {
                    let _ = handle_connection(
                        stream,
                        read_buffer_size,
                        task_metrics,
                        task_processor,
                        task_cluster,
                        task_owner_threads,
                        task_replication,
                    )
                    .await;
                });
            }
        }
    }

    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    expiration_task.abort();
    let _ = expiration_task.await;
    Ok(())
}
