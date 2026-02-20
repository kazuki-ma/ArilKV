use std::future::Future;
use std::io;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use garnet_cluster::ClusterConfigStore;
use tokio::net::TcpListener;
use tokio::task::JoinSet;

use crate::redis_replication::RedisReplicationCoordinator;
use crate::{
    build_owner_thread_pool, handle_connection, RequestProcessor, ServerConfig, ServerMetrics,
};

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
    run_listener_with_shutdown_and_cluster(
        listener,
        config.read_buffer_size,
        metrics,
        shutdown,
        cluster_config,
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
        io::Error::new(
            io::ErrorKind::Other,
            format!("request processor initialization failed: {err}"),
        )
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
    let mut tasks = JoinSet::new();
    let owner_thread_pool = build_owner_thread_pool(&processor)?;
    let replication = Arc::new(RedisReplicationCoordinator::new(Arc::clone(&processor)));
    let expiration_processor = Arc::clone(&processor);
    let expiration_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(50));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let _ = expiration_processor.expire_stale_keys(128);
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
