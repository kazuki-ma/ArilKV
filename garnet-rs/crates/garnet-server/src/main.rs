use garnet_server::{run, ServerConfig, ServerMetrics};
use std::sync::Arc;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> std::io::Result<()> {
    let config = ServerConfig::default();
    let metrics = Arc::new(ServerMetrics::default());
    run(config, metrics).await
}
