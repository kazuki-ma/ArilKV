use garnet_server::{run_listener_with_shutdown, ServerMetrics};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio::sync::oneshot;
use tokio::time::{sleep, timeout, Duration};

#[tokio::test]
async fn redis_cli_basic_command_compatibility() {
    if redis_cli_available().await.is_err() {
        return;
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let metrics = Arc::new(ServerMetrics::default());
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let server_metrics = Arc::clone(&metrics);
    let server = tokio::spawn(async move {
        run_listener_with_shutdown(listener, 4096, server_metrics, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
    });

    wait_for_server(port).await;
    assert_eq!(run_redis_cli(port, &["PING"]).await.unwrap(), "PONG");
    assert_eq!(
        run_redis_cli(port, &["SET", "foo", "bar"]).await.unwrap(),
        "OK"
    );
    assert_eq!(run_redis_cli(port, &["GET", "foo"]).await.unwrap(), "bar");
    assert_eq!(
        run_redis_cli(port, &["INCR", "counter"]).await.unwrap(),
        "1"
    );
    assert_eq!(
        run_redis_cli(port, &["INCR", "counter"]).await.unwrap(),
        "2"
    );
    assert_eq!(
        run_redis_cli(port, &["EXPIRE", "foo", "0"]).await.unwrap(),
        "1"
    );
    assert_eq!(run_redis_cli(port, &["TTL", "foo"]).await.unwrap(), "-2");
    assert_eq!(
        run_redis_cli(port, &["SET", "pfoo", "bar"]).await.unwrap(),
        "OK"
    );
    assert_eq!(
        run_redis_cli(port, &["PEXPIRE", "pfoo", "0"])
            .await
            .unwrap(),
        "1"
    );
    assert_eq!(run_redis_cli(port, &["PTTL", "pfoo"]).await.unwrap(), "-2");
    assert_eq!(
        run_redis_cli(port, &["SET", "persist", "bar"])
            .await
            .unwrap(),
        "OK"
    );
    assert_eq!(
        run_redis_cli(port, &["PEXPIRE", "persist", "1000"])
            .await
            .unwrap(),
        "1"
    );
    assert_eq!(
        run_redis_cli(port, &["PERSIST", "persist"]).await.unwrap(),
        "1"
    );
    assert_eq!(
        run_redis_cli(port, &["TTL", "persist"]).await.unwrap(),
        "-1"
    );
    assert_eq!(
        run_redis_cli(port, &["SET", "dfoo", "bar"]).await.unwrap(),
        "OK"
    );
    assert_eq!(run_redis_cli(port, &["DEL", "dfoo"]).await.unwrap(), "1");
    assert_eq!(run_redis_cli(port, &["GET", "dfoo"]).await.unwrap(), "");

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

async fn redis_cli_available() -> Result<(), String> {
    let output = timeout(
        Duration::from_secs(2),
        Command::new("redis-cli").arg("--version").output(),
    )
    .await
    .map_err(|_| "redis-cli --version timed out".to_string())?
    .map_err(|error| format!("failed to execute redis-cli: {error}"))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&output.stderr).to_string())
    }
}

async fn wait_for_server(port: u16) {
    for _ in 0..30 {
        if let Ok(result) = run_redis_cli(port, &["PING"]).await {
            if result == "PONG" {
                return;
            }
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("server did not become ready on port {}", port);
}

async fn run_redis_cli(port: u16, args: &[&str]) -> Result<String, String> {
    let mut command = Command::new("redis-cli");
    command.args([
        "-h",
        "127.0.0.1",
        "-p",
        &port.to_string(),
        "-2",
        "--raw",
        "-t",
        "1",
    ]);
    command.args(args);

    let output = timeout(Duration::from_secs(2), command.output())
        .await
        .map_err(|_| format!("redis-cli timed out for args: {:?}", args))?
        .map_err(|error| format!("failed to execute redis-cli {:?}: {}", args, error))?;

    if !output.status.success() {
        return Err(format!(
            "redis-cli {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}
