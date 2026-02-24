use super::*;
use garnet_cluster::{
    redis_hash_slot, AsyncGossipEngine, ChannelReplicationTransport, ClusterConfig,
    ClusterConfigStore, ClusterFailoverController, ClusterManager, FailoverCoordinator,
    FailureDetector, GossipCoordinator, GossipNode, InMemoryGossipTransport, ReplicationEvent,
    ReplicationManager, SlotState, Worker, WorkerRole, LOCAL_WORKER_ID,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;
use tokio::time::{sleep, Duration, Instant};

#[tokio::test]
async fn accept_loop_spawns_connection_handlers() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metrics = Arc::new(ServerMetrics::default());
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let server_metrics = Arc::clone(&metrics);
    let server = tokio::spawn(async move {
        run_listener_with_shutdown(listener, 1024, server_metrics, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
    });

    let mut client1 = TcpStream::connect(addr).await.unwrap();
    client1.write_all(b"PING").await.unwrap();
    drop(client1);

    let mut client2 = TcpStream::connect(addr).await.unwrap();
    client2.write_all(b"PONG").await.unwrap();
    drop(client2);

    wait_until(
        || metrics.closed_connections() >= 2 && metrics.bytes_received() >= 8,
        Duration::from_secs(1),
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();

    assert_eq!(metrics.accepted_connections(), 2);
    assert_eq!(metrics.active_connections(), 0);
    assert_eq!(metrics.closed_connections(), 2);
    assert!(metrics.bytes_received() >= 8);
}

#[tokio::test]
async fn shutdown_signal_stops_accept_loop() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let metrics = Arc::new(ServerMetrics::default());
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let server_metrics = Arc::clone(&metrics);
    let server = tokio::spawn(async move {
        run_listener_with_shutdown(listener, 512, server_metrics, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
    });

    let _ = shutdown_tx.send(());
    let joined = tokio::time::timeout(Duration::from_secs(1), server).await;
    assert!(joined.is_ok());
    assert_eq!(metrics.accepted_connections(), 0);
}

#[tokio::test]
async fn tcp_pipeline_executes_basic_crud_commands() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metrics = Arc::new(ServerMetrics::default());
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let server_metrics = Arc::clone(&metrics);
    let server = tokio::spawn(async move {
        run_listener_with_shutdown(listener, 1024, server_metrics, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
    });

    let mut client = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut client,
        b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*4\r\n$3\r\nSET\r\n$3\r\nkey\r\n$6\r\nvalue2\r\n$2\r\nNX\r\n",
        b"$-1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*4\r\n$3\r\nSET\r\n$3\r\nkey\r\n$7\r\nupdated\r\n$2\r\nXX\r\n",
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
        b"$7\r\nupdated\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*5\r\n$3\r\nSET\r\n$3\r\nttl\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$2\r\n10\r\n",
        b"+OK\r\n",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    send_and_expect(&mut client, b"*2\r\n$3\r\nGET\r\n$3\r\nttl\r\n", b"$-1\r\n").await;
    send_and_expect(
        &mut client,
        b"*3\r\n$3\r\nSET\r\n$6\r\nexpkey\r\n$5\r\nvalue\r\n",
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*3\r\n$6\r\nEXPIRE\r\n$6\r\nexpkey\r\n$1\r\n0\r\n",
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*2\r\n$4\r\nPTTL\r\n$6\r\nexpkey\r\n",
        b":-2\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*3\r\n$3\r\nSET\r\n$6\r\npexkey\r\n$5\r\nvalue\r\n",
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*3\r\n$7\r\nPEXPIRE\r\n$6\r\npexkey\r\n$1\r\n0\r\n",
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*2\r\n$3\r\nTTL\r\n$6\r\npexkey\r\n",
        b":-2\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*5\r\n$3\r\nSET\r\n$11\r\npersist-key\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$4\r\n1000\r\n",
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*2\r\n$7\r\nPERSIST\r\n$11\r\npersist-key\r\n",
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*2\r\n$3\r\nTTL\r\n$11\r\npersist-key\r\n",
        b":-1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*2\r\n$3\r\nDEL\r\n$11\r\npersist-key\r\n",
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*4\r\n$4\r\nHSET\r\n$4\r\nhkey\r\n$6\r\nfield1\r\n$2\r\nv1\r\n",
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*3\r\n$4\r\nHGET\r\n$4\r\nhkey\r\n$6\r\nfield1\r\n",
        b"$2\r\nv1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*2\r\n$7\r\nHGETALL\r\n$4\r\nhkey\r\n",
        b"*2\r\n$6\r\nfield1\r\n$2\r\nv1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*3\r\n$4\r\nHDEL\r\n$4\r\nhkey\r\n$6\r\nfield1\r\n",
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*2\r\n$7\r\nHGETALL\r\n$4\r\nhkey\r\n",
        b"*0\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*4\r\n$5\r\nLPUSH\r\n$4\r\nlkey\r\n$1\r\na\r\n$1\r\nb\r\n",
        b":2\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*4\r\n$6\r\nLRANGE\r\n$4\r\nlkey\r\n$1\r\n0\r\n$2\r\n-1\r\n",
        b"*2\r\n$1\r\nb\r\n$1\r\na\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*2\r\n$4\r\nRPOP\r\n$4\r\nlkey\r\n",
        b"$1\r\na\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*2\r\n$4\r\nLPOP\r\n$4\r\nlkey\r\n",
        b"$1\r\nb\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*2\r\n$4\r\nLPOP\r\n$4\r\nlkey\r\n",
        b"$-1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*4\r\n$4\r\nSADD\r\n$4\r\nskey\r\n$1\r\na\r\n$1\r\nb\r\n",
        b":2\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*3\r\n$9\r\nSISMEMBER\r\n$4\r\nskey\r\n$1\r\nb\r\n",
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*2\r\n$8\r\nSMEMBERS\r\n$4\r\nskey\r\n",
        b"*2\r\n$1\r\na\r\n$1\r\nb\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*4\r\n$4\r\nSREM\r\n$4\r\nskey\r\n$1\r\na\r\n$1\r\nb\r\n",
        b":2\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*2\r\n$8\r\nSMEMBERS\r\n$4\r\nskey\r\n",
        b"*0\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*6\r\n$4\r\nZADD\r\n$4\r\nzkey\r\n$1\r\n2\r\n$3\r\ntwo\r\n$1\r\n1\r\n$3\r\none\r\n",
        b":2\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*3\r\n$6\r\nZSCORE\r\n$4\r\nzkey\r\n$3\r\none\r\n",
        b"$1\r\n1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*4\r\n$6\r\nZRANGE\r\n$4\r\nzkey\r\n$1\r\n0\r\n$2\r\n-1\r\n",
        b"*2\r\n$3\r\none\r\n$3\r\ntwo\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*4\r\n$4\r\nZREM\r\n$4\r\nzkey\r\n$3\r\none\r\n$3\r\ntwo\r\n",
        b":2\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*4\r\n$6\r\nZRANGE\r\n$4\r\nzkey\r\n$1\r\n0\r\n$2\r\n-1\r\n",
        b"*0\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n",
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n",
        b":2\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*1\r\n$4\r\nEXEC\r\n",
        b"-ERR EXEC without MULTI\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*1\r\n$7\r\nDISCARD\r\n",
        b"-ERR DISCARD without MULTI\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*1\r\n$5\r\nWATCH\r\n",
        b"-ERR wrong number of arguments for 'WATCH' command\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*2\r\n$7\r\nUNWATCH\r\n$1\r\nx\r\n",
        b"-ERR wrong number of arguments for 'UNWATCH' command\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*2\r\n$6\r\nASKING\r\n$1\r\nx\r\n",
        b"-ERR wrong number of arguments for 'ASKING' command\r\n",
    )
    .await;
    send_and_expect(&mut client, b"*1\r\n$6\r\nASKING\r\n", b"+OK\r\n").await;
    send_and_expect(&mut client, b"*1\r\n$5\r\nMULTI\r\n", b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        b"*3\r\n$3\r\nSET\r\n$5\r\ntxkey\r\n$1\r\n1\r\n",
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*2\r\n$4\r\nINCR\r\n$5\r\ntxkey\r\n",
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(&mut client, b"*1\r\n$4\r\nEXEC\r\n", b"*2\r\n+OK\r\n:2\r\n").await;
    send_and_expect(
        &mut client,
        b"*2\r\n$3\r\nGET\r\n$5\r\ntxkey\r\n",
        b"$1\r\n2\r\n",
    )
    .await;
    send_and_expect(&mut client, b"*1\r\n$5\r\nMULTI\r\n", b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        b"*3\r\n$3\r\nSET\r\n$8\r\ndiscardk\r\n$1\r\nx\r\n",
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(&mut client, b"*1\r\n$7\r\nDISCARD\r\n", b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        b"*2\r\n$3\r\nGET\r\n$8\r\ndiscardk\r\n",
        b"$-1\r\n",
    )
    .await;
    let mut peer = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut client,
        b"*2\r\n$5\r\nWATCH\r\n$7\r\ntxwatch\r\n",
        b"+OK\r\n",
    )
    .await;
    send_and_expect(&mut client, b"*1\r\n$5\r\nMULTI\r\n", b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        b"*3\r\n$3\r\nSET\r\n$7\r\ntxwatch\r\n$5\r\ninner\r\n",
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut peer,
        b"*3\r\n$3\r\nSET\r\n$7\r\ntxwatch\r\n$5\r\nouter\r\n",
        b"+OK\r\n",
    )
    .await;
    send_and_expect(&mut client, b"*1\r\n$4\r\nEXEC\r\n", b"*-1\r\n").await;
    send_and_expect(
        &mut client,
        b"*2\r\n$3\r\nGET\r\n$7\r\ntxwatch\r\n",
        b"$5\r\nouter\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*2\r\n$5\r\nWATCH\r\n$7\r\ntxwatch\r\n",
        b"+OK\r\n",
    )
    .await;
    send_and_expect(&mut client, b"*1\r\n$7\r\nUNWATCH\r\n", b"+OK\r\n").await;
    send_and_expect(&mut client, b"*1\r\n$5\r\nMULTI\r\n", b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        b"*3\r\n$3\r\nSET\r\n$7\r\ntxwatch\r\n$5\r\ninner\r\n",
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut peer,
        b"*3\r\n$3\r\nSET\r\n$7\r\ntxwatch\r\n$6\r\nouter2\r\n",
        b"+OK\r\n",
    )
    .await;
    send_and_expect(&mut client, b"*1\r\n$4\r\nEXEC\r\n", b"*1\r\n+OK\r\n").await;
    send_and_expect(
        &mut client,
        b"*2\r\n$3\r\nGET\r\n$7\r\ntxwatch\r\n",
        b"$5\r\ninner\r\n",
    )
    .await;
    send_and_expect(&mut client, b"*1\r\n$5\r\nMULTI\r\n", b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        b"*2\r\n$5\r\nWATCH\r\n$7\r\ntxwatch\r\n",
        b"-ERR WATCH inside MULTI is not allowed\r\n",
    )
    .await;
    send_and_expect(&mut client, b"*1\r\n$7\r\nDISCARD\r\n", b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        b"*2\r\n$3\r\nDEL\r\n$5\r\ntxkey\r\n",
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*2\r\n$3\r\nDEL\r\n$7\r\ntxwatch\r\n",
        b":1\r\n",
    )
    .await;
    send_and_expect(&mut client, b"*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n", b":1\r\n").await;
    send_and_expect(&mut client, b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n", b"$-1\r\n").await;
    send_and_expect(&mut client, b"*1\r\n$6\r\nDBSIZE\r\n", b":1\r\n").await;
    let expected_command_response =
        encode_resp_command(crate::command_spec::command_names_for_command_response());
    send_and_expect(
        &mut client,
        b"*1\r\n$7\r\nCOMMAND\r\n",
        &expected_command_response,
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn tcp_inline_pipeline_executes_basic_crud_commands() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metrics = Arc::new(ServerMetrics::default());
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let server_metrics = Arc::clone(&metrics);
    let server = tokio::spawn(async move {
        run_listener_with_shutdown(listener, 1024, server_metrics, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
    });

    let mut client = TcpStream::connect(addr).await.unwrap();
    client
        .write_all(b"SET k1 xyzk\r\nGET k1\r\nPING\r\n")
        .await
        .unwrap();
    let expected = b"+OK\r\n$4\r\nxyzk\r\n+PONG\r\n";
    let mut actual = vec![0u8; expected.len()];
    tokio::time::timeout(Duration::from_secs(1), client.read_exact(&mut actual))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(actual, expected);

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn blocking_blpop_wakes_by_polling_from_another_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metrics = Arc::new(ServerMetrics::default());
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let server_metrics = Arc::clone(&metrics);
    let server = tokio::spawn(async move {
        run_listener_with_shutdown(listener, 1024, server_metrics, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
    });

    let mut waiter = TcpStream::connect(addr).await.unwrap();
    let mut producer = TcpStream::connect(addr).await.unwrap();
    waiter
        .write_all(b"*3\r\n$5\r\nBLPOP\r\n$1\r\nk\r\n$1\r\n0\r\n")
        .await
        .unwrap();

    let mut probe = [0u8; 1];
    assert!(
        tokio::time::timeout(Duration::from_millis(20), waiter.read(&mut probe))
            .await
            .is_err(),
        "blocking BLPOP should not return before producer pushes",
    );

    producer
        .write_all(b"*3\r\n$5\r\nRPUSH\r\n$1\r\nk\r\n$1\r\nv\r\n")
        .await
        .unwrap();

    let response = read_exact_with_timeout(&mut waiter, 18, Duration::from_secs(1)).await;
    assert_eq!(response, b"*2\r\n$1\r\nk\r\n$1\r\nv\r\n");

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn blocking_blpop_respects_timeout_without_updates() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metrics = Arc::new(ServerMetrics::default());
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let server_metrics = Arc::clone(&metrics);
    let server = tokio::spawn(async move {
        run_listener_with_shutdown(listener, 1024, server_metrics, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
    });

    let mut client = TcpStream::connect(addr).await.unwrap();
    client
        .write_all(b"*3\r\n$5\r\nBLPOP\r\n$1\r\nk\r\n$4\r\n0.05\r\n")
        .await
        .unwrap();

    let response = read_exact_with_timeout(&mut client, 5, Duration::from_secs(1)).await;
    assert_eq!(response, b"*-1\r\n");

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn blocking_clients_are_visible_in_info_and_client_list() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metrics = Arc::new(ServerMetrics::default());
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let server_metrics = Arc::clone(&metrics);
    let server = tokio::spawn(async move {
        run_listener_with_shutdown(listener, 1024, server_metrics, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
    });

    let mut waiter = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();
    let mut producer = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut waiter,
        b"*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$6\r\nwaiter\r\n",
        b"+OK\r\n",
    )
    .await;
    let waiter_id = send_and_read_integer(
        &mut waiter,
        b"*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n",
        Duration::from_secs(1),
    )
    .await;

    waiter
        .write_all(b"*3\r\n$5\r\nBLPOP\r\n$1\r\nk\r\n$1\r\n1\r\n")
        .await
        .unwrap();

    let waiter_id_text = waiter_id.to_string();
    let client_list_frame =
        encode_resp_command(&[b"CLIENT", b"LIST", b"ID", waiter_id_text.as_bytes()]);
    let mut blocked_visible = false;
    let deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < deadline {
        let info = send_and_read_bulk_payload(
            &mut inspector,
            b"*1\r\n$4\r\nINFO\r\n",
            Duration::from_secs(1),
        )
        .await;
        if !info
            .windows("blocked_clients:1".len())
            .any(|w| w == b"blocked_clients:1")
        {
            sleep(Duration::from_millis(10)).await;
            continue;
        }

        let list_payload =
            send_and_read_bulk_payload(&mut inspector, &client_list_frame, Duration::from_secs(1))
                .await;
        if list_payload
            .windows("flags=b".len())
            .any(|w| w == b"flags=b")
            && list_payload
                .windows("cmd=blpop".len())
                .any(|w| w == b"cmd=blpop")
            && list_payload
                .windows("name=waiter".len())
                .any(|w| w == b"name=waiter")
        {
            blocked_visible = true;
            break;
        }
        sleep(Duration::from_millis(10)).await;
    }
    assert!(
        blocked_visible,
        "blocked client should be visible via INFO/CLIENT LIST while waiting"
    );

    producer
        .write_all(b"*3\r\n$5\r\nRPUSH\r\n$1\r\nk\r\n$1\r\nv\r\n")
        .await
        .unwrap();
    let popped = read_exact_with_timeout(&mut waiter, 18, Duration::from_secs(1)).await;
    assert_eq!(popped, b"*2\r\n$1\r\nk\r\n$1\r\nv\r\n");

    let info_after = send_and_read_bulk_payload(
        &mut inspector,
        b"*1\r\n$4\r\nINFO\r\n",
        Duration::from_secs(1),
    )
    .await;
    assert!(
        info_after
            .windows("blocked_clients:0".len())
            .any(|w| w == b"blocked_clients:0"),
        "INFO should report blocked_clients:0 after wakeup: {}",
        String::from_utf8_lossy(&info_after),
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn client_unblock_unblocks_blocking_pop_with_timeout_and_error_modes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metrics = Arc::new(ServerMetrics::default());
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let server_metrics = Arc::clone(&metrics);
    let server = tokio::spawn(async move {
        run_listener_with_shutdown(listener, 1024, server_metrics, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
    });

    let mut waiter = TcpStream::connect(addr).await.unwrap();
    let mut controller = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();

    let waiter_id = send_and_read_integer(
        &mut waiter,
        b"*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n",
        Duration::from_secs(1),
    )
    .await;
    let waiter_id_text = waiter_id.to_string();

    waiter
        .write_all(b"*3\r\n$5\r\nBLPOP\r\n$1\r\nk\r\n$1\r\n0\r\n")
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;

    let unblock_timeout = encode_resp_command(&[b"CLIENT", b"UNBLOCK", waiter_id_text.as_bytes()]);
    let timeout_unblocked =
        send_and_read_integer(&mut controller, &unblock_timeout, Duration::from_secs(1)).await;
    assert_eq!(timeout_unblocked, 1);
    let timeout_response = read_exact_with_timeout(&mut waiter, 5, Duration::from_secs(1)).await;
    assert_eq!(timeout_response, b"*-1\r\n");

    waiter
        .write_all(b"*3\r\n$5\r\nBLPOP\r\n$1\r\nk\r\n$1\r\n0\r\n")
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;

    let unblock_error =
        encode_resp_command(&[b"CLIENT", b"UNBLOCK", waiter_id_text.as_bytes(), b"ERROR"]);
    let error_unblocked =
        send_and_read_integer(&mut controller, &unblock_error, Duration::from_secs(1)).await;
    assert_eq!(error_unblocked, 1);
    let error_response = read_resp_line_with_timeout(&mut waiter, Duration::from_secs(1)).await;
    assert_eq!(
        error_response,
        b"-UNBLOCKED client unblocked via CLIENT UNBLOCK"
    );

    let controller_id = send_and_read_integer(
        &mut controller,
        b"*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n",
        Duration::from_secs(1),
    )
    .await;
    let controller_id_text = controller_id.to_string();
    let unblock_non_blocked =
        encode_resp_command(&[b"CLIENT", b"UNBLOCK", controller_id_text.as_bytes()]);
    let non_blocked_result = send_and_read_integer(
        &mut controller,
        &unblock_non_blocked,
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(non_blocked_result, 0);

    controller
        .write_all(&encode_resp_command(&[b"CLIENT", b"UNBLOCK", b"asd"]))
        .await
        .unwrap();
    let invalid_id_response =
        read_resp_line_with_timeout(&mut controller, Duration::from_secs(1)).await;
    assert_eq!(
        invalid_id_response,
        b"-ERR value is not an integer or out of range"
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn blocking_pipeline_preserves_waiter_fairness() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metrics = Arc::new(ServerMetrics::default());
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let server_metrics = Arc::clone(&metrics);
    let server = tokio::spawn(async move {
        run_listener_with_shutdown(listener, 1024, server_metrics, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
    });

    let mut waiter = TcpStream::connect(addr).await.unwrap();
    let mut pipelined = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut inspector,
        b"*2\r\n$3\r\nDEL\r\n$6\r\nmylist\r\n",
        b":0\r\n",
    )
    .await;

    waiter
        .write_all(b"*3\r\n$5\r\nBLPOP\r\n$6\r\nmylist\r\n$1\r\n0\r\n")
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;

    pipelined
        .write_all(b"LPUSH mylist 1\r\nBLPOP mylist 0\r\n")
        .await
        .unwrap();
    let waiter_value = read_exact_with_timeout(&mut waiter, 23, Duration::from_secs(1)).await;
    assert_eq!(waiter_value, b"*2\r\n$6\r\nmylist\r\n$1\r\n1\r\n");
    let pipelined_lpush = read_exact_with_timeout(&mut pipelined, 4, Duration::from_secs(1)).await;
    assert_eq!(pipelined_lpush, b":1\r\n");

    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;

    send_and_expect(
        &mut inspector,
        b"*3\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$1\r\n2\r\n",
        b":1\r\n",
    )
    .await;
    wait_for_blocked_clients(&mut inspector, 0, Duration::from_secs(1)).await;

    let pipelined_blpop = read_exact_with_timeout(&mut pipelined, 23, Duration::from_secs(1)).await;
    assert_eq!(pipelined_blpop, b"*2\r\n$6\r\nmylist\r\n$1\r\n2\r\n");

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn blocking_list_wakeups_increase_rdb_changes_since_last_save() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metrics = Arc::new(ServerMetrics::default());
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let server_metrics = Arc::clone(&metrics);
    let server = tokio::spawn(async move {
        run_listener_with_shutdown(listener, 1024, server_metrics, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
    });

    let mut waiter = TcpStream::connect(addr).await.unwrap();
    let mut producer = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();

    let info_before = send_and_read_bulk_payload(
        &mut inspector,
        b"*1\r\n$4\r\nINFO\r\n",
        Duration::from_secs(1),
    )
    .await;
    let dirty_before = read_info_u64(&info_before, "rdb_changes_since_last_save").unwrap_or(0);

    waiter
        .write_all(b"*3\r\n$5\r\nBLPOP\r\n$6\r\nlst{t}\r\n$1\r\n0\r\n")
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;

    producer
        .write_all(b"*3\r\n$5\r\nLPUSH\r\n$6\r\nlst{t}\r\n$1\r\na\r\n")
        .await
        .unwrap();
    let popped = read_exact_with_timeout(&mut waiter, 23, Duration::from_secs(1)).await;
    assert_eq!(popped, b"*2\r\n$6\r\nlst{t}\r\n$1\r\na\r\n");

    let info_after_blpop = send_and_read_bulk_payload(
        &mut inspector,
        b"*1\r\n$4\r\nINFO\r\n",
        Duration::from_secs(1),
    )
    .await;
    let dirty_after_blpop =
        read_info_u64(&info_after_blpop, "rdb_changes_since_last_save").unwrap_or(0);
    assert_eq!(
        dirty_after_blpop.saturating_sub(dirty_before),
        2,
        "BLPOP wakeup path should increase dirty counter by two (LPUSH + unblocked pop)"
    );

    let info_before_blmove = info_after_blpop;
    let dirty_before_blmove =
        read_info_u64(&info_before_blmove, "rdb_changes_since_last_save").unwrap_or(0);

    waiter
        .write_all(
            b"*6\r\n$6\r\nBLMOVE\r\n$6\r\nlst{t}\r\n$7\r\nlst1{t}\r\n$4\r\nLEFT\r\n$4\r\nLEFT\r\n$1\r\n0\r\n",
        )
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;

    producer
        .write_all(b"*3\r\n$5\r\nLPUSH\r\n$6\r\nlst{t}\r\n$1\r\nb\r\n")
        .await
        .unwrap();
    let moved = read_exact_with_timeout(&mut waiter, 7, Duration::from_secs(1)).await;
    assert_eq!(moved, b"$1\r\nb\r\n");

    let info_after_blmove = send_and_read_bulk_payload(
        &mut inspector,
        b"*1\r\n$4\r\nINFO\r\n",
        Duration::from_secs(1),
    )
    .await;
    let dirty_after_blmove =
        read_info_u64(&info_after_blmove, "rdb_changes_since_last_save").unwrap_or(0);
    assert_eq!(
        dirty_after_blmove.saturating_sub(dirty_before_blmove),
        2,
        "BLMOVE wakeup path should increase dirty counter by two (LPUSH + unblocked move)"
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn linked_blmove_chain_is_observable_without_intermediate_residue() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metrics = Arc::new(ServerMetrics::default());
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let server_metrics = Arc::clone(&metrics);
    let server = tokio::spawn(async move {
        run_listener_with_shutdown(listener, 1024, server_metrics, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
    });

    let mut waiter1 = TcpStream::connect(addr).await.unwrap();
    let mut waiter2 = TcpStream::connect(addr).await.unwrap();
    let mut producer = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut inspector,
        b"*4\r\n$3\r\nDEL\r\n$8\r\nlist1{t}\r\n$8\r\nlist2{t}\r\n$8\r\nlist3{t}\r\n",
        b":0\r\n",
    )
    .await;

    waiter1
        .write_all(
            b"*6\r\n$6\r\nBLMOVE\r\n$8\r\nlist1{t}\r\n$8\r\nlist2{t}\r\n$5\r\nRIGHT\r\n$4\r\nLEFT\r\n$1\r\n0\r\n",
        )
        .await
        .unwrap();
    waiter2
        .write_all(
            b"*6\r\n$6\r\nBLMOVE\r\n$8\r\nlist2{t}\r\n$8\r\nlist3{t}\r\n$4\r\nLEFT\r\n$5\r\nRIGHT\r\n$1\r\n0\r\n",
        )
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 2, Duration::from_secs(1)).await;

    send_and_expect(
        &mut producer,
        b"*3\r\n$5\r\nRPUSH\r\n$8\r\nlist1{t}\r\n$3\r\nfoo\r\n",
        b":1\r\n",
    )
    .await;

    send_and_expect(
        &mut inspector,
        b"*4\r\n$6\r\nLRANGE\r\n$8\r\nlist1{t}\r\n$1\r\n0\r\n$2\r\n-1\r\n",
        b"*0\r\n",
    )
    .await;
    send_and_expect(
        &mut inspector,
        b"*4\r\n$6\r\nLRANGE\r\n$8\r\nlist2{t}\r\n$1\r\n0\r\n$2\r\n-1\r\n",
        b"*0\r\n",
    )
    .await;
    send_and_expect(
        &mut inspector,
        b"*4\r\n$6\r\nLRANGE\r\n$8\r\nlist3{t}\r\n$1\r\n0\r\n$2\r\n-1\r\n",
        b"*1\r\n$3\r\nfoo\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn brpoplpush_wakeup_invalidates_watch_before_exec() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metrics = Arc::new(ServerMetrics::default());
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let server_metrics = Arc::clone(&metrics);
    let server = tokio::spawn(async move {
        run_listener_with_shutdown(listener, 1024, server_metrics, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
    });

    let mut blocked_client = TcpStream::connect(addr).await.unwrap();
    let mut watching_client = TcpStream::connect(addr).await.unwrap();
    let mut producer = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut producer,
        b"*4\r\n$3\r\nDEL\r\n$10\r\nsrclist{t}\r\n$10\r\ndstlist{t}\r\n$10\r\nsomekey{t}\r\n",
        b":0\r\n",
    )
    .await;
    send_and_expect(
        &mut producer,
        b"*3\r\n$3\r\nSET\r\n$10\r\nsomekey{t}\r\n$9\r\nsomevalue\r\n",
        b"+OK\r\n",
    )
    .await;

    blocked_client
        .write_all(
            b"*4\r\n$10\r\nBRPOPLPUSH\r\n$10\r\nsrclist{t}\r\n$10\r\ndstlist{t}\r\n$1\r\n0\r\n",
        )
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;

    send_and_expect(
        &mut watching_client,
        b"*2\r\n$5\r\nWATCH\r\n$10\r\ndstlist{t}\r\n",
        b"+OK\r\n",
    )
    .await;
    send_and_expect(&mut watching_client, b"*1\r\n$5\r\nMULTI\r\n", b"+OK\r\n").await;
    send_and_expect(
        &mut watching_client,
        b"*2\r\n$3\r\nGET\r\n$10\r\nsomekey{t}\r\n",
        b"+QUEUED\r\n",
    )
    .await;

    send_and_expect(
        &mut producer,
        b"*3\r\n$5\r\nLPUSH\r\n$10\r\nsrclist{t}\r\n$7\r\nelement\r\n",
        b":1\r\n",
    )
    .await;
    send_and_expect(&mut watching_client, b"*1\r\n$4\r\nEXEC\r\n", b"*-1\r\n").await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn info_commandstats_counts_blocking_command_once() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metrics = Arc::new(ServerMetrics::default());
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let server_metrics = Arc::clone(&metrics);
    let server = tokio::spawn(async move {
        run_listener_with_shutdown(listener, 1024, server_metrics, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
    });

    let mut waiter = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();
    let mut producer = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut inspector,
        b"*2\r\n$6\r\nCONFIG\r\n$9\r\nRESETSTAT\r\n",
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut inspector,
        b"*2\r\n$3\r\nDEL\r\n$6\r\nmylist\r\n",
        b":0\r\n",
    )
    .await;

    waiter
        .write_all(b"*3\r\n$5\r\nBLPOP\r\n$6\r\nmylist\r\n$1\r\n0\r\n")
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;

    send_and_expect(
        &mut producer,
        b"*3\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$1\r\n1\r\n",
        b":1\r\n",
    )
    .await;
    let _ = read_exact_with_timeout(&mut waiter, 23, Duration::from_secs(1)).await;

    let payload = send_and_read_bulk_payload(
        &mut inspector,
        b"*2\r\n$4\r\nINFO\r\n$12\r\nCOMMANDSTATS\r\n",
        Duration::from_secs(1),
    )
    .await;
    let payload_text = String::from_utf8_lossy(&payload);
    assert!(
        payload_text.contains("cmdstat_blpop:calls=1"),
        "unexpected INFO COMMANDSTATS payload: {payload_text}"
    );
    assert!(
        payload_text.contains("rejected_calls=0,failed_calls=0"),
        "unexpected INFO COMMANDSTATS payload: {payload_text}"
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn replicaof_enables_replication_and_no_one_promotes_back_to_master() {
    let master_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let master_addr = master_listener.local_addr().unwrap();
    let replica_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let replica_addr = replica_listener.local_addr().unwrap();

    let master_metrics = Arc::new(ServerMetrics::default());
    let replica_metrics = Arc::new(ServerMetrics::default());
    let (master_shutdown_tx, master_shutdown_rx) = oneshot::channel::<()>();
    let (replica_shutdown_tx, replica_shutdown_rx) = oneshot::channel::<()>();

    let master_metrics_task = Arc::clone(&master_metrics);
    let master_server = tokio::spawn(async move {
        run_listener_with_shutdown(master_listener, 1024, master_metrics_task, async move {
            let _ = master_shutdown_rx.await;
        })
        .await
        .unwrap();
    });

    let replica_metrics_task = Arc::clone(&replica_metrics);
    let replica_server = tokio::spawn(async move {
        run_listener_with_shutdown(replica_listener, 1024, replica_metrics_task, async move {
            let _ = replica_shutdown_rx.await;
        })
        .await
        .unwrap();
    });

    let mut master_client = TcpStream::connect(master_addr).await.unwrap();
    let mut replica_client = TcpStream::connect(replica_addr).await.unwrap();

    let master_port = master_addr.port().to_string();
    let slaveof = encode_resp_command(&[b"SLAVEOF", b"127.0.0.1", master_port.as_bytes()]);
    send_and_expect(&mut replica_client, &slaveof, b"+OK\r\n").await;

    let mut replicated = false;
    for attempt in 0..50 {
        let value = format!("v{attempt}");
        let set_frame = encode_resp_command(&[b"SET", b"repl:test", value.as_bytes()]);
        send_and_expect(&mut master_client, &set_frame, b"+OK\r\n").await;

        let get_frame = encode_resp_command(&[b"GET", b"repl:test"]);
        replica_client.write_all(&get_frame).await.unwrap();
        let mut response = [0u8; 64];
        let bytes_read = tokio::time::timeout(
            Duration::from_millis(200),
            replica_client.read(&mut response),
        )
        .await
        .unwrap()
        .unwrap();
        let expected = format!("${}\r\n{}\r\n", value.len(), value);
        if &response[..bytes_read] == expected.as_bytes() {
            replicated = true;
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert!(replicated, "replica did not receive replicated value");

    send_and_expect(
        &mut replica_client,
        b"*3\r\n$3\r\nSET\r\n$13\r\nrepl:readonly\r\n$1\r\nx\r\n",
        b"-READONLY You can't write against a read only replica.\r\n",
    )
    .await;

    send_and_expect(
        &mut replica_client,
        b"*3\r\n$9\r\nREPLICAOF\r\n$2\r\nNO\r\n$3\r\nONE\r\n",
        b"+OK\r\n",
    )
    .await;

    send_and_expect(
        &mut replica_client,
        b"*3\r\n$3\r\nSET\r\n$13\r\nrepl:readonly\r\n$1\r\ny\r\n",
        b"+OK\r\n",
    )
    .await;

    let _ = master_shutdown_tx.send(());
    let _ = replica_shutdown_tx.send(());
    master_server.await.unwrap();
    replica_server.await.unwrap();
}

#[tokio::test]
async fn replicaof_replication_rewrites_evalsha_after_replica_cache_flush() {
    let master_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let master_addr = master_listener.local_addr().unwrap();
    let replica_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let replica_addr = replica_listener.local_addr().unwrap();

    let master_metrics = Arc::new(ServerMetrics::default());
    let replica_metrics = Arc::new(ServerMetrics::default());
    let (master_shutdown_tx, master_shutdown_rx) = oneshot::channel::<()>();
    let (replica_shutdown_tx, replica_shutdown_rx) = oneshot::channel::<()>();

    let master_processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());
    let replica_processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());

    let master_metrics_task = Arc::clone(&master_metrics);
    let master_server = tokio::spawn(async move {
        run_listener_with_shutdown_and_cluster_with_processor(
            master_listener,
            1024,
            master_metrics_task,
            async move {
                let _ = master_shutdown_rx.await;
            },
            None,
            master_processor,
        )
        .await
        .unwrap();
    });

    let replica_metrics_task = Arc::clone(&replica_metrics);
    let replica_server = tokio::spawn(async move {
        run_listener_with_shutdown_and_cluster_with_processor(
            replica_listener,
            1024,
            replica_metrics_task,
            async move {
                let _ = replica_shutdown_rx.await;
            },
            None,
            replica_processor,
        )
        .await
        .unwrap();
    });

    let mut master_client = TcpStream::connect(master_addr).await.unwrap();
    let mut replica_client = TcpStream::connect(replica_addr).await.unwrap();

    let master_port = master_addr.port().to_string();
    let slaveof = encode_resp_command(&[b"SLAVEOF", b"127.0.0.1", master_port.as_bytes()]);
    send_and_expect(&mut replica_client, &slaveof, b"+OK\r\n").await;

    let script = b"redis.call('SET', KEYS[1], ARGV[1]); return ARGV[1]";
    let sha = send_and_read_bulk_payload(
        &mut master_client,
        &encode_resp_command(&[b"SCRIPT", b"LOAD", script]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(sha.len(), 40);

    let key = b"repl:lua:key";
    let mut replicated_v1 = false;
    for _ in 0..50 {
        let evalsha_v1 = encode_resp_command(&[b"EVALSHA", sha.as_slice(), b"1", key, b"v1"]);
        send_and_expect(&mut master_client, &evalsha_v1, b"$2\r\nv1\r\n").await;

        let get_frame = encode_resp_command(&[b"GET", key]);
        replica_client.write_all(&get_frame).await.unwrap();
        let mut response = [0u8; 64];
        let bytes_read = tokio::time::timeout(
            Duration::from_millis(200),
            replica_client.read(&mut response),
        )
        .await
        .unwrap()
        .unwrap();
        if &response[..bytes_read] == b"$2\r\nv1\r\n" {
            replicated_v1 = true;
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert!(replicated_v1, "replica did not receive first evalsha write");

    send_and_expect(
        &mut replica_client,
        b"*3\r\n$9\r\nREPLICAOF\r\n$2\r\nNO\r\n$3\r\nONE\r\n",
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut replica_client,
        b"*2\r\n$6\r\nSCRIPT\r\n$5\r\nFLUSH\r\n",
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut replica_client,
        &encode_resp_command(&[b"EVALSHA", sha.as_slice(), b"1", key, b"probe"]),
        b"-NOSCRIPT No matching script. Please use EVAL.\r\n",
    )
    .await;

    send_and_expect(&mut replica_client, &slaveof, b"+OK\r\n").await;

    let mut replicated_v2 = false;
    for _ in 0..80 {
        let evalsha_v2 = encode_resp_command(&[b"EVALSHA", sha.as_slice(), b"1", key, b"v2"]);
        send_and_expect(&mut master_client, &evalsha_v2, b"$2\r\nv2\r\n").await;

        let get_frame = encode_resp_command(&[b"GET", key]);
        replica_client.write_all(&get_frame).await.unwrap();
        let mut response = [0u8; 64];
        let bytes_read = tokio::time::timeout(
            Duration::from_millis(200),
            replica_client.read(&mut response),
        )
        .await
        .unwrap()
        .unwrap();
        if &response[..bytes_read] == b"$2\r\nv2\r\n" {
            replicated_v2 = true;
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert!(
        replicated_v2,
        "replica did not receive second evalsha write"
    );

    let _ = master_shutdown_tx.send(());
    let _ = replica_shutdown_tx.send(());
    master_server.await.unwrap();
    replica_server.await.unwrap();
}

#[tokio::test]
async fn replicaof_replication_propagates_function_load_and_fcall() {
    let master_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let master_addr = master_listener.local_addr().unwrap();
    let replica_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let replica_addr = replica_listener.local_addr().unwrap();

    let master_metrics = Arc::new(ServerMetrics::default());
    let replica_metrics = Arc::new(ServerMetrics::default());
    let (master_shutdown_tx, master_shutdown_rx) = oneshot::channel::<()>();
    let (replica_shutdown_tx, replica_shutdown_rx) = oneshot::channel::<()>();

    let master_processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());
    let replica_processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());

    let master_metrics_task = Arc::clone(&master_metrics);
    let master_server = tokio::spawn(async move {
        run_listener_with_shutdown_and_cluster_with_processor(
            master_listener,
            1024,
            master_metrics_task,
            async move {
                let _ = master_shutdown_rx.await;
            },
            None,
            master_processor,
        )
        .await
        .unwrap();
    });

    let replica_metrics_task = Arc::clone(&replica_metrics);
    let replica_server = tokio::spawn(async move {
        run_listener_with_shutdown_and_cluster_with_processor(
            replica_listener,
            1024,
            replica_metrics_task,
            async move {
                let _ = replica_shutdown_rx.await;
            },
            None,
            replica_processor,
        )
        .await
        .unwrap();
    });

    let mut master_client = TcpStream::connect(master_addr).await.unwrap();
    let mut replica_client = TcpStream::connect(replica_addr).await.unwrap();

    let master_port = master_addr.port().to_string();
    let slaveof = encode_resp_command(&[b"SLAVEOF", b"127.0.0.1", master_port.as_bytes()]);
    send_and_expect(&mut replica_client, &slaveof, b"+OK\r\n").await;

    let library_source = b"#!lua name=lib_repl\nredis.register_function{function_name='rw_set', callback=function(keys, args) return redis.call('SET', keys[1], args[1]) end}\nredis.register_function{function_name='ro_get', callback=function(keys, args) return redis.call('GET', keys[1]) end, flags={'no-writes'}}";
    let mut function_replicated = false;
    for _ in 0..80 {
        send_and_expect(
            &mut master_client,
            &encode_resp_command(&[b"FUNCTION", b"LOAD", b"REPLACE", library_source]),
            b"$8\r\nlib_repl\r\n",
        )
        .await;

        let probe = encode_resp_command(&[b"FCALL_RO", b"ro_get", b"1", b"repl:function:probe"]);
        replica_client.write_all(&probe).await.unwrap();
        let mut response = [0u8; 128];
        let bytes_read = tokio::time::timeout(
            Duration::from_millis(200),
            replica_client.read(&mut response),
        )
        .await
        .unwrap()
        .unwrap();
        if &response[..bytes_read] == b"$-1\r\n" {
            function_replicated = true;
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert!(
        function_replicated,
        "replica did not receive replicated function library"
    );

    let key = b"repl:function:key";
    let mut replicated = false;
    for _ in 0..80 {
        let fcall_rw = encode_resp_command(&[b"FCALL", b"rw_set", b"1", key, b"v1"]);
        send_and_expect(&mut master_client, &fcall_rw, b"+OK\r\n").await;

        let fcall_ro = encode_resp_command(&[b"FCALL_RO", b"ro_get", b"1", key]);
        replica_client.write_all(&fcall_ro).await.unwrap();
        let mut response = [0u8; 128];
        let bytes_read = tokio::time::timeout(
            Duration::from_millis(200),
            replica_client.read(&mut response),
        )
        .await
        .unwrap()
        .unwrap();
        if &response[..bytes_read] == b"$2\r\nv1\r\n" {
            replicated = true;
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }

    assert!(
        replicated,
        "replica did not receive function library and fcall updates"
    );

    let _ = master_shutdown_tx.send(());
    let _ = replica_shutdown_tx.send(());
    master_server.await.unwrap();
    replica_server.await.unwrap();
}

#[tokio::test]
async fn sync_replication_stream_starts_with_select_db_zero() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metrics = Arc::new(ServerMetrics::default());
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let server_metrics = Arc::clone(&metrics);
    let server = tokio::spawn(async move {
        run_listener_with_shutdown(listener, 1024, server_metrics, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
    });

    let mut replica_stream = TcpStream::connect(addr).await.unwrap();
    replica_stream.write_all(b"SYNC\r\n").await.unwrap();

    let header = read_resp_line_with_timeout(&mut replica_stream, Duration::from_secs(1)).await;
    assert!(
        header.starts_with(b"$"),
        "SYNC response must start with bulk RDB length, got: {}",
        String::from_utf8_lossy(&header)
    );
    let payload_len = std::str::from_utf8(&header[1..])
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let _payload =
        read_exact_with_timeout(&mut replica_stream, payload_len, Duration::from_secs(1)).await;

    let expected_select = b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n";
    let select_frame = read_exact_with_timeout(
        &mut replica_stream,
        expected_select.len(),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(select_frame, expected_select);

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn cluster_routing_returns_moved_for_remote_slots() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metrics = Arc::new(ServerMetrics::default());
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let mut cluster_config = ClusterConfig::new_local("local", "127.0.0.1", 6379);
    let remote_worker = Worker::new("remote", "10.0.0.2", 6380, WorkerRole::Primary);
    let (with_remote, remote_id) = cluster_config.add_worker(remote_worker).unwrap();
    cluster_config = with_remote;

    let local_key = b"local-k";
    let local_slot = redis_hash_slot(local_key);
    cluster_config = cluster_config
        .set_slot_state(local_slot, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let remote_key = b"remote-k";
    let remote_slot = redis_hash_slot(remote_key);
    cluster_config = cluster_config
        .set_slot_state(remote_slot, remote_id, SlotState::Stable)
        .unwrap();

    let ask_key = b"ask-k";
    let ask_slot = redis_hash_slot(ask_key);
    cluster_config = cluster_config
        .set_slot_state(ask_slot, remote_id, SlotState::Importing)
        .unwrap();

    let tx_key_a = b"tx-slot-a";
    let tx_slot_a = redis_hash_slot(tx_key_a);
    let mut tx_key_b = b"tx-slot-b".to_vec();
    let mut tx_slot_b = redis_hash_slot(&tx_key_b);
    while tx_slot_b == tx_slot_a {
        tx_key_b.push(b'x');
        tx_slot_b = redis_hash_slot(&tx_key_b);
    }
    cluster_config = cluster_config
        .set_slot_state(tx_slot_a, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();
    cluster_config = cluster_config
        .set_slot_state(tx_slot_b, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut unbound_key = b"unbound-k".to_vec();
    let mut unbound_slot = redis_hash_slot(&unbound_key);
    while [local_slot, remote_slot, ask_slot, tx_slot_a, tx_slot_b].contains(&unbound_slot) {
        unbound_key.push(b'x');
        unbound_slot = redis_hash_slot(&unbound_key);
    }
    let cluster_store = Arc::new(ClusterConfigStore::new(cluster_config));

    let server_metrics = Arc::clone(&metrics);
    let server_cluster = Arc::clone(&cluster_store);
    let server = tokio::spawn(async move {
        run_listener_with_shutdown_and_cluster(
            listener,
            1024,
            server_metrics,
            async move {
                let _ = shutdown_rx.await;
            },
            Some(server_cluster),
        )
        .await
        .unwrap();
    });

    let mut client = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut client,
        b"*3\r\n$3\r\nSET\r\n$7\r\nlocal-k\r\n$2\r\nok\r\n",
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*2\r\n$3\r\nGET\r\n$7\r\nlocal-k\r\n",
        b"$2\r\nok\r\n",
    )
    .await;

    let expected_moved = format!("-MOVED {} 10.0.0.2:6380\r\n", remote_slot);
    send_and_expect(
        &mut client,
        b"*2\r\n$3\r\nGET\r\n$8\r\nremote-k\r\n",
        expected_moved.as_bytes(),
    )
    .await;
    send_and_expect(
        &mut client,
        b"*3\r\n$3\r\nDEL\r\n$7\r\nlocal-k\r\n$8\r\nremote-k\r\n",
        b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
    )
    .await;
    let expected_ask = format!("-ASK {} 10.0.0.2:6380\r\n", ask_slot);
    send_and_expect(
        &mut client,
        b"*2\r\n$3\r\nGET\r\n$5\r\nask-k\r\n",
        expected_ask.as_bytes(),
    )
    .await;
    send_and_expect(
        &mut client,
        b"*3\r\n$5\r\nWATCH\r\n$7\r\nlocal-k\r\n$8\r\nremote-k\r\n",
        b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
    )
    .await;
    send_and_expect(&mut client, b"*1\r\n$6\r\nASKING\r\n", b"+OK\r\n").await;
    send_and_expect(&mut client, b"*1\r\n$4\r\nPING\r\n", b"+PONG\r\n").await;
    send_and_expect(
        &mut client,
        b"*2\r\n$3\r\nGET\r\n$5\r\nask-k\r\n",
        expected_ask.as_bytes(),
    )
    .await;
    send_and_expect(&mut client, b"*1\r\n$6\r\nASKING\r\n", b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        b"*2\r\n$3\r\nGET\r\n$5\r\nask-k\r\n",
        b"$-1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*2\r\n$3\r\nGET\r\n$5\r\nask-k\r\n",
        expected_ask.as_bytes(),
    )
    .await;
    send_and_expect(&mut client, b"*1\r\n$5\r\nMULTI\r\n", b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        b"*3\r\n$3\r\nSET\r\n$9\r\ntx-slot-a\r\n$1\r\n1\r\n",
        b"+QUEUED\r\n",
    )
    .await;
    let tx_b_len = tx_key_b.len();
    let tx_b_req = format!(
        "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n$1\r\n2\r\n",
        tx_b_len,
        String::from_utf8(tx_key_b.clone()).unwrap()
    );
    send_and_expect(
        &mut client,
        tx_b_req.as_bytes(),
        b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*1\r\n$4\r\nEXEC\r\n",
        b"-EXECABORT Transaction discarded because of previous errors.\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*2\r\n$3\r\nGET\r\n$9\r\ntx-slot-a\r\n",
        b"$-1\r\n",
    )
    .await;
    let unbound_req = format!(
        "*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n",
        unbound_key.len(),
        String::from_utf8(unbound_key.clone()).unwrap()
    );
    let expected_clusterdown = format!("-CLUSTERDOWN Hash slot {} is unbound\r\n", unbound_slot);
    send_and_expect(
        &mut client,
        unbound_req.as_bytes(),
        expected_clusterdown.as_bytes(),
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn cluster_multi_node_slot_routing_and_failover_updates_redirections() {
    let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr1 = listener1.local_addr().unwrap();
    let listener2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = listener2.local_addr().unwrap();
    let listener3 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr3 = listener3.local_addr().unwrap();

    let key1 = b"node1-key".to_vec();
    let slot1 = redis_hash_slot(&key1);
    let mut key2 = b"node2-key".to_vec();
    let mut slot2 = redis_hash_slot(&key2);
    while slot2 == slot1 {
        key2.push(b'x');
        slot2 = redis_hash_slot(&key2);
    }
    let mut key3 = b"node3-key".to_vec();
    let mut slot3 = redis_hash_slot(&key3);
    while slot3 == slot1 || slot3 == slot2 {
        key3.push(b'x');
        slot3 = redis_hash_slot(&key3);
    }

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", addr1.port());
    let (next1, node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            addr2.port(),
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1;
    let (next1, node3_id_in_1) = config1
        .add_worker(Worker::new(
            "node-3",
            "127.0.0.1",
            addr3.port(),
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1;
    config1 = config1
        .set_slot_state(slot1, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();
    config1 = config1
        .set_slot_state(slot2, node2_id_in_1, SlotState::Stable)
        .unwrap();
    config1 = config1
        .set_slot_state(slot3, node3_id_in_1, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", addr2.port());
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            addr1.port(),
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2;
    let (next2, node3_id_in_2) = config2
        .add_worker(Worker::new(
            "node-3",
            "127.0.0.1",
            addr3.port(),
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2;
    config2 = config2
        .set_slot_state(slot1, node1_id_in_2, SlotState::Stable)
        .unwrap();
    config2 = config2
        .set_slot_state(slot2, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();
    config2 = config2
        .set_slot_state(slot3, node3_id_in_2, SlotState::Stable)
        .unwrap();

    let mut config3 = ClusterConfig::new_local("node-3", "127.0.0.1", addr3.port());
    let (next3, node1_id_in_3) = config3
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            addr1.port(),
            WorkerRole::Primary,
        ))
        .unwrap();
    config3 = next3;
    let (next3, node2_id_in_3) = config3
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            addr2.port(),
            WorkerRole::Primary,
        ))
        .unwrap();
    config3 = next3;
    config3 = config3
        .set_slot_state(slot1, node1_id_in_3, SlotState::Stable)
        .unwrap();
    config3 = config3
        .set_slot_state(slot2, node2_id_in_3, SlotState::Stable)
        .unwrap();
    config3 = config3
        .set_slot_state(slot3, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let store1 = Arc::new(ClusterConfigStore::new(config1));
    let store2 = Arc::new(ClusterConfigStore::new(config2));
    let store3 = Arc::new(ClusterConfigStore::new(config3));

    let (shutdown1_tx, shutdown1_rx) = oneshot::channel::<()>();
    let (shutdown2_tx, shutdown2_rx) = oneshot::channel::<()>();
    let (shutdown3_tx, shutdown3_rx) = oneshot::channel::<()>();

    let server1_metrics = Arc::new(ServerMetrics::default());
    let server1_cluster = Arc::clone(&store1);
    let server1 = tokio::spawn(async move {
        run_listener_with_shutdown_and_cluster(
            listener1,
            1024,
            server1_metrics,
            async move {
                let _ = shutdown1_rx.await;
            },
            Some(server1_cluster),
        )
        .await
        .unwrap();
    });

    let server2_metrics = Arc::new(ServerMetrics::default());
    let server2_cluster = Arc::clone(&store2);
    let server2 = tokio::spawn(async move {
        run_listener_with_shutdown_and_cluster(
            listener2,
            1024,
            server2_metrics,
            async move {
                let _ = shutdown2_rx.await;
            },
            Some(server2_cluster),
        )
        .await
        .unwrap();
    });

    let server3_metrics = Arc::new(ServerMetrics::default());
    let server3_cluster = Arc::clone(&store3);
    let server3 = tokio::spawn(async move {
        run_listener_with_shutdown_and_cluster(
            listener3,
            1024,
            server3_metrics,
            async move {
                let _ = shutdown3_rx.await;
            },
            Some(server3_cluster),
        )
        .await
        .unwrap();
    });

    let mut node1 = TcpStream::connect(addr1).await.unwrap();
    let mut node2 = TcpStream::connect(addr2).await.unwrap();
    let mut node3 = TcpStream::connect(addr3).await.unwrap();

    let set_key1 = encode_resp_command(&[b"SET", &key1, b"v1"]);
    let get_key1 = encode_resp_command(&[b"GET", &key1]);
    let set_key2 = encode_resp_command(&[b"SET", &key2, b"v2"]);
    let get_key2 = encode_resp_command(&[b"GET", &key2]);
    let set_key3 = encode_resp_command(&[b"SET", &key3, b"v3"]);
    let get_key3 = encode_resp_command(&[b"GET", &key3]);

    send_and_expect(&mut node1, &set_key1, b"+OK\r\n").await;
    send_and_expect(&mut node1, &get_key1, b"$2\r\nv1\r\n").await;
    send_and_expect(&mut node2, &set_key2, b"+OK\r\n").await;
    send_and_expect(&mut node2, &get_key2, b"$2\r\nv2\r\n").await;
    send_and_expect(&mut node3, &set_key3, b"+OK\r\n").await;
    send_and_expect(&mut node3, &get_key3, b"$2\r\nv3\r\n").await;

    let moved_to_node1 = format!("-MOVED {} 127.0.0.1:{}\r\n", slot1, addr1.port());
    let moved_to_node2 = format!("-MOVED {} 127.0.0.1:{}\r\n", slot2, addr2.port());
    let moved_to_node3 = format!("-MOVED {} 127.0.0.1:{}\r\n", slot3, addr3.port());

    send_and_expect(&mut node1, &get_key2, moved_to_node2.as_bytes()).await;
    send_and_expect(&mut node1, &get_key3, moved_to_node3.as_bytes()).await;
    send_and_expect(&mut node2, &get_key1, moved_to_node1.as_bytes()).await;
    send_and_expect(&mut node3, &get_key1, moved_to_node1.as_bytes()).await;

    let mut replication1 = ReplicationManager::new(Some(7), 1_000, 2_000).unwrap();
    let mut replication2 = ReplicationManager::new(Some(7), 1_000, 2_000).unwrap();
    let mut replication3 = ReplicationManager::new(Some(7), 1_000, 2_000).unwrap();
    replication1.record_replica_offset(node3_id_in_1, 1_950);
    replication2.record_replica_offset(node3_id_in_2, 1_950);
    replication3.record_replica_offset(LOCAL_WORKER_ID, 1_950);
    let mut coordinator1 = FailoverCoordinator::new();
    let mut coordinator2 = FailoverCoordinator::new();
    let mut coordinator3 = FailoverCoordinator::new();

    let failover_input1 = store1
        .load()
        .as_ref()
        .clone()
        .set_worker_replica_of(node3_id_in_1, "node-2")
        .unwrap();
    store1.publish(failover_input1);
    let plan1 = coordinator1
        .execute_for_failed_primary(&store1, &replication1, "node-2")
        .unwrap()
        .expect("node-1 config should elect node-3");
    assert_eq!(plan1.promoted_worker_id, node3_id_in_1);

    let failover_input2 = store2
        .load()
        .as_ref()
        .clone()
        .set_worker_replica_of(node3_id_in_2, "node-2")
        .unwrap();
    store2.publish(failover_input2);
    let plan2 = coordinator2
        .execute_for_failed_primary(&store2, &replication2, "node-2")
        .unwrap()
        .expect("node-2 config should elect node-3");
    assert_eq!(plan2.promoted_worker_id, node3_id_in_2);

    let failover_input3 = store3
        .load()
        .as_ref()
        .clone()
        .set_worker_replica_of(LOCAL_WORKER_ID, "node-2")
        .unwrap();
    store3.publish(failover_input3);
    let plan3 = coordinator3
        .execute_for_failed_primary(&store3, &replication3, "node-2")
        .unwrap()
        .expect("node-3 config should elect local node");
    assert_eq!(plan3.failed_primary_worker_id, node2_id_in_3);
    assert_eq!(plan3.promoted_worker_id, LOCAL_WORKER_ID);

    let moved_to_node3_after_failover = format!("-MOVED {} 127.0.0.1:{}\r\n", slot2, addr3.port());
    send_and_expect(
        &mut node1,
        &get_key2,
        moved_to_node3_after_failover.as_bytes(),
    )
    .await;
    send_and_expect(
        &mut node2,
        &get_key2,
        moved_to_node3_after_failover.as_bytes(),
    )
    .await;
    send_and_expect(&mut node3, &get_key2, b"$-1\r\n").await;
    let set_key2_failover = encode_resp_command(&[b"SET", &key2, b"v2f"]);
    send_and_expect(&mut node3, &set_key2_failover, b"+OK\r\n").await;
    send_and_expect(&mut node3, &get_key2, b"$3\r\nv2f\r\n").await;

    let _ = shutdown1_tx.send(());
    let _ = shutdown2_tx.send(());
    let _ = shutdown3_tx.send(());
    server1.await.unwrap();
    server2.await.unwrap();
    server3.await.unwrap();
}

#[tokio::test]
async fn cluster_manager_failover_loop_updates_server_redirections() {
    let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr1 = listener1.local_addr().unwrap();
    let listener3 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr3 = listener3.local_addr().unwrap();
    let listener2_for_port = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = listener2_for_port.local_addr().unwrap();

    let key2 = b"node2-key".to_vec();
    let slot2 = redis_hash_slot(&key2);

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", addr1.port());
    let (next1, node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            addr2.port(),
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1;
    let (next1, node3_id_in_1) = config1
        .add_worker(Worker::new(
            "node-3",
            "127.0.0.1",
            addr3.port(),
            WorkerRole::Replica,
        ))
        .unwrap();
    config1 = next1;
    config1 = config1
        .set_worker_replica_of(node3_id_in_1, "node-2")
        .unwrap()
        .set_slot_state(slot2, node2_id_in_1, SlotState::Stable)
        .unwrap();

    let mut config3 = ClusterConfig::new_local("node-3", "127.0.0.1", addr3.port())
        .set_local_worker_role(WorkerRole::Replica)
        .unwrap()
        .set_worker_replica_of(LOCAL_WORKER_ID, "node-2")
        .unwrap();
    let (next3, node1_id_in_3) = config3
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            addr1.port(),
            WorkerRole::Primary,
        ))
        .unwrap();
    config3 = next3;
    let (next3, node2_id_in_3) = config3
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            addr2.port(),
            WorkerRole::Primary,
        ))
        .unwrap();
    config3 = next3;
    config3 = config3
        .set_slot_state(slot2, node2_id_in_3, SlotState::Stable)
        .unwrap()
        .set_slot_state(
            redis_hash_slot(b"node1-anchor"),
            node1_id_in_3,
            SlotState::Stable,
        )
        .unwrap();

    let store1 = Arc::new(ClusterConfigStore::new(config1));
    let store3 = Arc::new(ClusterConfigStore::new(config3));

    let (shutdown1_tx, shutdown1_rx) = oneshot::channel::<()>();
    let (shutdown3_tx, shutdown3_rx) = oneshot::channel::<()>();

    let server1_metrics = Arc::new(ServerMetrics::default());
    let server1_cluster = Arc::clone(&store1);
    let server1 = tokio::spawn(async move {
        run_listener_with_shutdown_and_cluster(
            listener1,
            1024,
            server1_metrics,
            async move {
                let _ = shutdown1_rx.await;
            },
            Some(server1_cluster),
        )
        .await
        .unwrap();
    });

    let server3_metrics = Arc::new(ServerMetrics::default());
    let server3_cluster = Arc::clone(&store3);
    let server3 = tokio::spawn(async move {
        run_listener_with_shutdown_and_cluster(
            listener3,
            1024,
            server3_metrics,
            async move {
                let _ = shutdown3_rx.await;
            },
            Some(server3_cluster),
        )
        .await
        .unwrap();
    });

    let mut node1 = TcpStream::connect(addr1).await.unwrap();
    let mut node3 = TcpStream::connect(addr3).await.unwrap();
    let get_key2 = encode_resp_command(&[b"GET", &key2]);
    let moved_to_node2 = format!("-MOVED {} 127.0.0.1:{}\r\n", slot2, addr2.port());
    send_and_expect(&mut node1, &get_key2, moved_to_node2.as_bytes()).await;
    send_and_expect(&mut node3, &get_key2, moved_to_node2.as_bytes()).await;

    let gossip_nodes1 = vec![GossipNode::new(node2_id_in_1, 0)];
    let gossip_nodes3 = vec![GossipNode::new(node2_id_in_3, 0)];
    let gossip_engine1 = AsyncGossipEngine::new(
        GossipCoordinator::new(gossip_nodes1, 1),
        InMemoryGossipTransport::new(Arc::clone(&store1)),
        100,
        0,
    );
    let gossip_engine3 = AsyncGossipEngine::new(
        GossipCoordinator::new(gossip_nodes3, 1),
        InMemoryGossipTransport::new(Arc::clone(&store3)),
        100,
        0,
    );
    let mut manager1 = ClusterManager::new(gossip_engine1, Duration::from_millis(5));
    let mut manager3 = ClusterManager::new(gossip_engine3, Duration::from_millis(5));

    let mut detector1 = FailureDetector::new(1);
    let mut detector3 = FailureDetector::new(1);
    let mut controller1 = ClusterFailoverController::new();
    let mut controller3 = ClusterFailoverController::new();
    let mut replication1 = ReplicationManager::new(Some(7), 2_000, 2_200).unwrap();
    let mut replication3 = ReplicationManager::new(Some(7), 2_000, 2_200).unwrap();
    replication1.record_replica_offset(node3_id_in_1, 1_950);
    replication3.record_replica_offset(LOCAL_WORKER_ID, 1_950);

    let (repl1_tx, mut repl1_rx) = tokio::sync::mpsc::unbounded_channel::<ReplicationEvent>();
    let (repl3_tx, mut repl3_rx) = tokio::sync::mpsc::unbounded_channel::<ReplicationEvent>();
    let mut repl_transport1 = ChannelReplicationTransport::new(repl1_tx, 1_980);
    let mut repl_transport3 = ChannelReplicationTransport::new(repl3_tx, 1_980);
    let (_updates1_tx, updates1_rx) = tokio::sync::mpsc::unbounded_channel::<ClusterConfig>();
    let (_updates3_tx, updates3_rx) = tokio::sync::mpsc::unbounded_channel::<ClusterConfig>();

    let reports1 = manager1
        .run_with_config_updates_and_failover(
            &store1,
            updates1_rx,
            &mut detector1,
            &mut controller1,
            &mut replication1,
            &mut repl_transport1,
            tokio::time::sleep(Duration::from_millis(20)),
        )
        .await
        .unwrap();
    let reports3 = manager3
        .run_with_config_updates_and_failover(
            &store3,
            updates3_rx,
            &mut detector3,
            &mut controller3,
            &mut replication3,
            &mut repl_transport3,
            tokio::time::sleep(Duration::from_millis(20)),
        )
        .await
        .unwrap();
    assert!(reports1
        .iter()
        .any(|report| report.failed_worker_ids.contains(&node2_id_in_1)));
    assert!(reports3
        .iter()
        .any(|report| report.failed_worker_ids.contains(&node2_id_in_3)));
    assert_eq!(
        repl1_rx.recv().await,
        Some(ReplicationEvent::Checkpoint {
            worker_id: node3_id_in_1,
            checkpoint_id: 7,
        })
    );
    assert_eq!(
        repl3_rx.recv().await,
        Some(ReplicationEvent::Checkpoint {
            worker_id: LOCAL_WORKER_ID,
            checkpoint_id: 7,
        })
    );

    let moved_to_node3 = format!("-MOVED {} 127.0.0.1:{}\r\n", slot2, addr3.port());
    send_and_expect(&mut node1, &get_key2, moved_to_node3.as_bytes()).await;
    send_and_expect(&mut node3, &get_key2, b"$-1\r\n").await;
    let set_key2 = encode_resp_command(&[b"SET", &key2, b"v2-after-failover"]);
    send_and_expect(&mut node3, &set_key2, b"+OK\r\n").await;
    send_and_expect(&mut node3, &get_key2, b"$17\r\nv2-after-failover\r\n").await;

    let _ = shutdown1_tx.send(());
    let _ = shutdown3_tx.send(());
    server1.await.unwrap();
    server3.await.unwrap();
}

#[tokio::test]
async fn run_listener_with_cluster_control_plane_runs_server_and_detected_migration() {
    let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr1 = listener1.local_addr().unwrap();
    let listener2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = listener2.local_addr().unwrap();

    let key = b"{listener-cp}k".to_vec();
    let slot = redis_hash_slot(&key);
    let get_key = encode_resp_command(&[b"GET", &key]);

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", addr1.port());
    let (next1, node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            addr2.port(),
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1
        .set_slot_state(slot, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", addr2.port());
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            addr1.port(),
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .set_slot_state(slot, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let store1 = Arc::new(ClusterConfigStore::new(config1));
    let store2 = Arc::new(ClusterConfigStore::new(config2));
    let target_processor = Arc::new(RequestProcessor::new().unwrap());
    let (shutdown2_tx, shutdown2_rx) = oneshot::channel::<()>();
    let server2_metrics = Arc::new(ServerMetrics::default());
    let server2_store = Arc::clone(&store2);
    let server2_processor = Arc::clone(&target_processor);
    let server2 = tokio::spawn(async move {
        run_listener_with_shutdown_and_cluster_with_processor(
            listener2,
            1024,
            server2_metrics,
            async move {
                let _ = shutdown2_rx.await;
            },
            Some(server2_store),
            server2_processor,
        )
        .await
        .unwrap();
    });

    let (shutdown1_tx, shutdown1_rx) = oneshot::channel::<()>();
    let server1_metrics = Arc::new(ServerMetrics::default());
    let server1_store = Arc::clone(&store1);
    let server1_target_store = Arc::clone(&store2);
    let server1_target_processor = Arc::clone(&target_processor);
    let server1 = tokio::spawn(async move {
        let gossip_engine = AsyncGossipEngine::new(
            GossipCoordinator::new(Vec::new(), 1),
            InMemoryGossipTransport::new(Arc::clone(&server1_store)),
            100,
            0,
        );
        let mut manager = ClusterManager::new(gossip_engine, Duration::from_millis(2));
        let mut failure_detector = FailureDetector::new(1_000);
        let mut failover_controller = ClusterFailoverController::new();
        let mut replication_manager = ReplicationManager::new(Some(7), 2_000, 2_200).unwrap();
        let (repl_tx, mut repl_rx) = tokio::sync::mpsc::unbounded_channel::<ReplicationEvent>();
        let mut replication_transport = ChannelReplicationTransport::new(repl_tx, 1_980);
        let (_updates_tx, updates_rx) = tokio::sync::mpsc::unbounded_channel::<ClusterConfig>();
        let report = run_listener_with_cluster_control_plane(
            listener1,
            1024,
            server1_metrics,
            Arc::clone(&server1_store),
            &mut manager,
            server1_target_store.as_ref(),
            server1_target_processor.as_ref(),
            updates_rx,
            &mut failure_detector,
            &mut failover_controller,
            &mut replication_manager,
            &mut replication_transport,
            1,
            Duration::from_millis(1),
            Duration::from_millis(1),
            async move {
                let _ = shutdown1_rx.await;
            },
        )
        .await
        .unwrap();
        assert!(report.failover_report.failover_records.is_empty());
        assert!(report.failover_report.gossip_reports.len() > 0);
        assert!(report.migration_reports.len() > 0);
        assert!(repl_rx.try_recv().is_err());
        report
    });

    let mut node1 = TcpStream::connect(addr1).await.unwrap();
    let mut node2 = TcpStream::connect(addr2).await.unwrap();
    let set_key = encode_resp_command(&[b"SET", &key, b"value"]);
    send_and_expect(&mut node1, &set_key, b"+OK\r\n").await;
    send_and_expect(&mut node1, &get_key, b"$5\r\nvalue\r\n").await;

    let moved_to_node1 = format!("-MOVED {} 127.0.0.1:{}\r\n", slot, addr1.port());
    send_and_expect(&mut node2, &get_key, moved_to_node1.as_bytes()).await;

    let source_migrating = store1
        .load()
        .as_ref()
        .clone()
        .begin_slot_migration_to(slot, node2_id_in_1)
        .unwrap();
    store1.publish(source_migrating);
    let target_importing = store2
        .load()
        .as_ref()
        .clone()
        .begin_slot_import_from(slot, node1_id_in_2)
        .unwrap();
    store2.publish(target_importing);

    wait_until(
        || {
            store1.load().slot_state(slot).unwrap() == SlotState::Stable
                && store1.load().slot_assigned_owner(slot).unwrap() == node2_id_in_1
                && execute_processor_frame(target_processor.as_ref(), &get_key)
                    == b"$5\r\nvalue\r\n"
        },
        Duration::from_secs(1),
    )
    .await;

    let moved_to_node2 = format!("-MOVED {} 127.0.0.1:{}\r\n", slot, addr2.port());
    send_and_expect(&mut node1, &get_key, moved_to_node2.as_bytes()).await;
    send_and_expect(&mut node2, &get_key, b"$5\r\nvalue\r\n").await;

    let _ = shutdown1_tx.send(());
    let _ = shutdown2_tx.send(());
    let report = server1.await.unwrap();
    assert_eq!(report.migration_reports.len(), 1);
    server2.await.unwrap();
}

#[tokio::test]
async fn run_listener_with_cluster_control_plane_propagates_control_plane_errors() {
    let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr1 = listener1.local_addr().unwrap();

    let source_config = ClusterConfig::new_local("node-1", "127.0.0.1", addr1.port());
    let target_config = ClusterConfig::new_local("node-2", "127.0.0.1", 8752);
    let source_store = Arc::new(ClusterConfigStore::new(source_config));
    let target_store = ClusterConfigStore::new(target_config);
    let target_processor = RequestProcessor::new().unwrap();

    let gossip_engine = AsyncGossipEngine::new(
        GossipCoordinator::new(Vec::new(), 1),
        InMemoryGossipTransport::new(Arc::clone(&source_store)),
        100,
        0,
    );
    let mut manager = ClusterManager::new(gossip_engine, Duration::from_millis(2));
    let mut failure_detector = FailureDetector::new(1_000);
    let mut failover_controller = ClusterFailoverController::new();
    let mut replication_manager = ReplicationManager::new(Some(7), 2_000, 2_200).unwrap();
    let (repl_tx, _repl_rx) = tokio::sync::mpsc::unbounded_channel::<ReplicationEvent>();
    let mut replication_transport = ChannelReplicationTransport::new(repl_tx, 1_980);
    let (_updates_tx, updates_rx) = tokio::sync::mpsc::unbounded_channel::<ClusterConfig>();

    let result = run_listener_with_cluster_control_plane(
        listener1,
        1024,
        Arc::new(ServerMetrics::default()),
        Arc::clone(&source_store),
        &mut manager,
        &target_store,
        &target_processor,
        updates_rx,
        &mut failure_detector,
        &mut failover_controller,
        &mut replication_manager,
        &mut replication_transport,
        1,
        Duration::from_millis(1),
        Duration::from_millis(1),
        tokio::time::sleep(Duration::from_secs(1)),
    )
    .await;
    assert!(matches!(
        result,
        Err(ClusteredServerRunError::ControlPlane(
            ClusterManagerFailoverMigrationError::Migration(
                LiveSlotMigrationError::MissingSourcePeerNode(node_id)
            )
        )) if node_id == "node-2"
    ));
}

#[tokio::test]
async fn run_with_cluster_control_plane_binds_and_serves_requests() {
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr1 = probe.local_addr().unwrap();
    drop(probe);

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", addr1.port());
    let (config1_next, _node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            8702,
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = config1_next;

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", 8702);
    let (config2_next, _node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            addr1.port(),
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = config2_next;

    let store1 = Arc::new(ClusterConfigStore::new(config1));
    let store2 = Arc::new(ClusterConfigStore::new(config2));
    let target_processor = RequestProcessor::new().unwrap();
    let metrics = Arc::new(ServerMetrics::default());
    let client_addr = addr1;
    let client = tokio::spawn(async move {
        for _ in 0..50 {
            if let Ok(mut stream) = TcpStream::connect(client_addr).await {
                send_and_expect(&mut stream, b"*1\r\n$4\r\nPING\r\n", b"+PONG\r\n").await;
                return;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        panic!("failed to connect to clustered server test listener");
    });

    let gossip_engine = AsyncGossipEngine::new(
        GossipCoordinator::new(Vec::new(), 1),
        InMemoryGossipTransport::new(Arc::clone(&store1)),
        100,
        0,
    );
    let mut manager = ClusterManager::new(gossip_engine, Duration::from_millis(2));
    let mut failure_detector = FailureDetector::new(1_000);
    let mut failover_controller = ClusterFailoverController::new();
    let mut replication_manager = ReplicationManager::new(Some(7), 2_000, 2_200).unwrap();
    let (repl_tx, mut repl_rx) = tokio::sync::mpsc::unbounded_channel::<ReplicationEvent>();
    let mut replication_transport = ChannelReplicationTransport::new(repl_tx, 1_980);
    let (_updates_tx, updates_rx) = tokio::sync::mpsc::unbounded_channel::<ClusterConfig>();

    let report = run_with_cluster_control_plane(
        ServerConfig {
            bind_addr: addr1,
            read_buffer_size: 1024,
        },
        metrics,
        Arc::clone(&store1),
        &mut manager,
        store2.as_ref(),
        &target_processor,
        updates_rx,
        &mut failure_detector,
        &mut failover_controller,
        &mut replication_manager,
        &mut replication_transport,
        1,
        Duration::from_millis(1),
        Duration::from_millis(1),
        tokio::time::sleep(Duration::from_millis(30)),
    )
    .await
    .unwrap();
    client.await.unwrap();

    assert!(report.failover_report.gossip_reports.len() > 0);
    assert!(report.failover_report.failover_records.is_empty());
    assert!(report.migration_reports.is_empty());
    assert!(repl_rx.try_recv().is_err());
}

#[tokio::test]
async fn run_with_cluster_control_plane_propagates_bind_errors() {
    let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = occupied.local_addr().unwrap();

    let source_store = Arc::new(ClusterConfigStore::new(ClusterConfig::new_local(
        "node-1",
        "127.0.0.1",
        addr.port(),
    )));
    let target_store =
        ClusterConfigStore::new(ClusterConfig::new_local("node-2", "127.0.0.1", 8802));
    let target_processor = RequestProcessor::new().unwrap();
    let gossip_engine = AsyncGossipEngine::new(
        GossipCoordinator::new(Vec::new(), 1),
        InMemoryGossipTransport::new(Arc::clone(&source_store)),
        100,
        0,
    );
    let mut manager = ClusterManager::new(gossip_engine, Duration::from_millis(2));
    let mut failure_detector = FailureDetector::new(1_000);
    let mut failover_controller = ClusterFailoverController::new();
    let mut replication_manager = ReplicationManager::new(Some(7), 2_000, 2_200).unwrap();
    let (repl_tx, _repl_rx) = tokio::sync::mpsc::unbounded_channel::<ReplicationEvent>();
    let mut replication_transport = ChannelReplicationTransport::new(repl_tx, 1_980);
    let (_updates_tx, updates_rx) = tokio::sync::mpsc::unbounded_channel::<ClusterConfig>();

    let result = run_with_cluster_control_plane(
        ServerConfig {
            bind_addr: addr,
            read_buffer_size: 1024,
        },
        Arc::new(ServerMetrics::default()),
        source_store,
        &mut manager,
        &target_store,
        &target_processor,
        updates_rx,
        &mut failure_detector,
        &mut failover_controller,
        &mut replication_manager,
        &mut replication_transport,
        1,
        Duration::from_millis(1),
        Duration::from_millis(1),
        tokio::time::sleep(Duration::from_millis(10)),
    )
    .await;
    assert!(matches!(result, Err(ClusteredServerRunError::Io(_))));
}

#[tokio::test]
async fn cluster_manager_failover_and_detected_migration_runner_executes_migration_loop() {
    let key = b"{manager-detected}k".to_vec();
    let slot = redis_hash_slot(&key);

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", 8401);
    let (next1, node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            8402,
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1
        .set_slot_state(slot, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", 8402);
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            8401,
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .set_slot_state(slot, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let store1 = Arc::new(ClusterConfigStore::new(config1));
    let store2 = Arc::new(ClusterConfigStore::new(config2));
    let source_processor = RequestProcessor::new().unwrap();
    let target_processor = RequestProcessor::new().unwrap();

    assert_eq!(
        execute_processor_frame(
            &source_processor,
            &encode_resp_command(&[b"SET", &key, b"value"])
        ),
        b"+OK\r\n"
    );
    let source_migrating = store1
        .load()
        .as_ref()
        .clone()
        .begin_slot_migration_to(slot, node2_id_in_1)
        .unwrap();
    store1.publish(source_migrating);
    let target_importing = store2
        .load()
        .as_ref()
        .clone()
        .begin_slot_import_from(slot, node1_id_in_2)
        .unwrap();
    store2.publish(target_importing);

    let gossip_engine = AsyncGossipEngine::new(
        GossipCoordinator::new(Vec::new(), 1),
        InMemoryGossipTransport::new(Arc::clone(&store1)),
        100,
        0,
    );
    let mut manager = ClusterManager::new(gossip_engine, Duration::from_millis(2));
    let mut failure_detector = FailureDetector::new(1_000);
    let mut failover_controller = ClusterFailoverController::new();
    let mut replication_manager = ReplicationManager::new(Some(7), 2_000, 2_200).unwrap();
    let (repl_tx, mut repl_rx) = tokio::sync::mpsc::unbounded_channel::<ReplicationEvent>();
    let mut replication_transport = ChannelReplicationTransport::new(repl_tx, 1_980);
    let (_updates_tx, updates_rx) = tokio::sync::mpsc::unbounded_channel::<ClusterConfig>();

    let report = run_cluster_manager_with_config_updates_failover_and_detected_migrations(
        &mut manager,
        &store1,
        &store2,
        &source_processor,
        &target_processor,
        updates_rx,
        &mut failure_detector,
        &mut failover_controller,
        &mut replication_manager,
        &mut replication_transport,
        1,
        Duration::from_millis(1),
        Duration::from_millis(1),
        tokio::time::sleep(Duration::from_millis(20)),
    )
    .await
    .unwrap();
    assert!(!report.failover_report.gossip_reports.is_empty());
    assert!(report.failover_report.failover_records.is_empty());
    assert!(repl_rx.try_recv().is_err());
    assert_eq!(
        report.migration_reports,
        vec![LiveSlotMigrationsRunReport {
            slots: vec![LiveSlotMigrationSlotReport {
                slot,
                batches: 1,
                moved_keys: 1,
                finalized: true,
            }],
            interrupted: false,
        }]
    );

    let get_key = encode_resp_command(&[b"GET", &key]);
    assert_eq!(
        execute_processor_frame(&source_processor, &get_key),
        b"$-1\r\n"
    );
    assert_eq!(
        execute_processor_frame(&target_processor, &get_key),
        b"$5\r\nvalue\r\n"
    );
    assert_eq!(
        store1.load().slot_assigned_owner(slot).unwrap(),
        node2_id_in_1
    );
    assert_eq!(
        store2.load().slot_assigned_owner(slot).unwrap(),
        LOCAL_WORKER_ID
    );
}

#[tokio::test]
async fn cluster_manager_failover_and_detected_migration_runner_propagates_migration_errors() {
    let source_store = Arc::new(ClusterConfigStore::new(ClusterConfig::new_local(
        "node-1",
        "127.0.0.1",
        8501,
    )));
    let target_config = ClusterConfig::new_local("node-2", "127.0.0.1", 8502);
    let (target_config, _node1_id_in_2) = target_config
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            8501,
            WorkerRole::Primary,
        ))
        .unwrap();
    let target_store = Arc::new(ClusterConfigStore::new(target_config));
    let source_processor = RequestProcessor::new().unwrap();
    let target_processor = RequestProcessor::new().unwrap();

    let gossip_engine = AsyncGossipEngine::new(
        GossipCoordinator::new(Vec::new(), 1),
        InMemoryGossipTransport::new(Arc::clone(&source_store)),
        100,
        0,
    );
    let mut manager = ClusterManager::new(gossip_engine, Duration::from_millis(2));
    let mut failure_detector = FailureDetector::new(1_000);
    let mut failover_controller = ClusterFailoverController::new();
    let mut replication_manager = ReplicationManager::new(Some(7), 2_000, 2_200).unwrap();
    let (repl_tx, _repl_rx) = tokio::sync::mpsc::unbounded_channel::<ReplicationEvent>();
    let mut replication_transport = ChannelReplicationTransport::new(repl_tx, 1_980);
    let (_updates_tx, updates_rx) = tokio::sync::mpsc::unbounded_channel::<ClusterConfig>();

    let result = run_cluster_manager_with_config_updates_failover_and_detected_migrations(
        &mut manager,
        &source_store,
        &target_store,
        &source_processor,
        &target_processor,
        updates_rx,
        &mut failure_detector,
        &mut failover_controller,
        &mut replication_manager,
        &mut replication_transport,
        1,
        Duration::from_millis(1),
        Duration::from_millis(1),
        tokio::time::sleep(Duration::from_millis(50)),
    )
    .await;
    assert!(matches!(
        result,
        Err(ClusterManagerFailoverMigrationError::Migration(
            LiveSlotMigrationError::MissingSourcePeerNode(node_id)
        )) if node_id == "node-2"
    ));
}

#[tokio::test]
async fn cluster_manager_failover_and_detected_migration_runner_propagates_failover_errors() {
    let slot = 860u16;
    let mut source_config = ClusterConfig::new_local("node-1", "127.0.0.1", 8601)
        .set_local_worker_role(WorkerRole::Replica)
        .unwrap()
        .set_worker_replica_of(LOCAL_WORKER_ID, "node-2")
        .unwrap();
    let (source_config_next, node2_id_in_1) = source_config
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            8602,
            WorkerRole::Primary,
        ))
        .unwrap();
    source_config = source_config_next
        .set_slot_state(slot, node2_id_in_1, SlotState::Stable)
        .unwrap();

    let mut target_config = ClusterConfig::new_local("node-2", "127.0.0.1", 8602);
    let (target_config_next, node1_id_in_2) = target_config
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            8601,
            WorkerRole::Replica,
        ))
        .unwrap();
    target_config = target_config_next
        .set_slot_state(slot, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let source_store = Arc::new(ClusterConfigStore::new(source_config));
    let target_store = Arc::new(ClusterConfigStore::new(target_config));
    let source_processor = RequestProcessor::new().unwrap();
    let target_processor = RequestProcessor::new().unwrap();

    let gossip_engine = AsyncGossipEngine::new(
        GossipCoordinator::new(vec![GossipNode::new(node2_id_in_1, 0)], 1),
        InMemoryGossipTransport::new(Arc::clone(&source_store)),
        100,
        0,
    );
    let mut manager = ClusterManager::new(gossip_engine, Duration::from_millis(2));
    let mut failure_detector = FailureDetector::new(1);
    let mut failover_controller = ClusterFailoverController::new();
    let mut replication_manager = ReplicationManager::new(Some(7), 2_000, 2_200).unwrap();
    let (repl_tx, repl_rx) = tokio::sync::mpsc::unbounded_channel::<ReplicationEvent>();
    drop(repl_rx);
    let mut replication_transport = ChannelReplicationTransport::new(repl_tx, 1_980);
    let (_updates_tx, updates_rx) = tokio::sync::mpsc::unbounded_channel::<ClusterConfig>();

    let result = run_cluster_manager_with_config_updates_failover_and_detected_migrations(
        &mut manager,
        &source_store,
        &target_store,
        &source_processor,
        &target_processor,
        updates_rx,
        &mut failure_detector,
        &mut failover_controller,
        &mut replication_manager,
        &mut replication_transport,
        1,
        Duration::from_millis(1),
        Duration::from_millis(1),
        tokio::time::sleep(Duration::from_millis(50)),
    )
    .await;
    assert!(matches!(
        result,
        Err(ClusterManagerFailoverMigrationError::Failover(
            garnet_cluster::FailoverControllerError::Replication(
                garnet_cluster::ReplicationSyncError::Transport(
                    garnet_cluster::ChannelReplicationTransportError::ChannelClosed
                )
            )
        ))
    ));
}

#[tokio::test]
async fn cluster_live_slot_migration_transfers_data_and_updates_redirections() {
    let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr1 = listener1.local_addr().unwrap();
    let listener2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = listener2.local_addr().unwrap();

    let key = b"live-migrate-key".to_vec();
    let slot = redis_hash_slot(&key);

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", addr1.port());
    let (next1, node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            addr2.port(),
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1
        .set_slot_state(slot, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", addr2.port());
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            addr1.port(),
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .set_slot_state(slot, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let store1 = Arc::new(ClusterConfigStore::new(config1));
    let store2 = Arc::new(ClusterConfigStore::new(config2));
    let source_processor = Arc::new(RequestProcessor::new().unwrap());
    let target_processor = Arc::new(RequestProcessor::new().unwrap());

    let (shutdown1_tx, shutdown1_rx) = oneshot::channel::<()>();
    let (shutdown2_tx, shutdown2_rx) = oneshot::channel::<()>();

    let server1_metrics = Arc::new(ServerMetrics::default());
    let server1_cluster = Arc::clone(&store1);
    let server1_processor = Arc::clone(&source_processor);
    let server1 = tokio::spawn(async move {
        run_listener_with_shutdown_and_cluster_with_processor(
            listener1,
            1024,
            server1_metrics,
            async move {
                let _ = shutdown1_rx.await;
            },
            Some(server1_cluster),
            server1_processor,
        )
        .await
        .unwrap();
    });

    let server2_metrics = Arc::new(ServerMetrics::default());
    let server2_cluster = Arc::clone(&store2);
    let server2_processor = Arc::clone(&target_processor);
    let server2 = tokio::spawn(async move {
        run_listener_with_shutdown_and_cluster_with_processor(
            listener2,
            1024,
            server2_metrics,
            async move {
                let _ = shutdown2_rx.await;
            },
            Some(server2_cluster),
            server2_processor,
        )
        .await
        .unwrap();
    });

    let mut node1 = TcpStream::connect(addr1).await.unwrap();
    let mut node2 = TcpStream::connect(addr2).await.unwrap();
    let set_key = encode_resp_command(&[b"SET", &key, b"value"]);
    let get_key = encode_resp_command(&[b"GET", &key]);

    send_and_expect(&mut node1, &set_key, b"+OK\r\n").await;
    send_and_expect(&mut node1, &get_key, b"$5\r\nvalue\r\n").await;

    let moved_to_node1 = format!("-MOVED {} 127.0.0.1:{}\r\n", slot, addr1.port());
    send_and_expect(&mut node2, &get_key, moved_to_node1.as_bytes()).await;

    let migration_source = store1
        .load()
        .as_ref()
        .clone()
        .begin_slot_migration_to(slot, node2_id_in_1)
        .unwrap();
    store1.publish(migration_source);
    let migration_target = store2
        .load()
        .as_ref()
        .clone()
        .begin_slot_import_from(slot, node1_id_in_2)
        .unwrap();
    store2.publish(migration_target);

    let ask_to_node2 = format!("-ASK {} 127.0.0.1:{}\r\n", slot, addr2.port());
    send_and_expect(&mut node1, &get_key, ask_to_node2.as_bytes()).await;
    let ask_to_node1 = format!("-ASK {} 127.0.0.1:{}\r\n", slot, addr1.port());
    send_and_expect(&mut node2, &get_key, ask_to_node1.as_bytes()).await;

    send_and_expect(&mut node2, b"*1\r\n$6\r\nASKING\r\n", b"+OK\r\n").await;
    send_and_expect(&mut node2, &get_key, b"$-1\r\n").await;

    let moved = source_processor
        .migrate_slot_to(&target_processor, slot, 16, true)
        .unwrap();
    assert_eq!(moved, 1);

    send_and_expect(&mut node2, b"*1\r\n$6\r\nASKING\r\n", b"+OK\r\n").await;
    send_and_expect(&mut node2, &get_key, b"$5\r\nvalue\r\n").await;

    let finalized_source = store1
        .load()
        .as_ref()
        .clone()
        .finalize_slot_migration(slot, node2_id_in_1)
        .unwrap();
    store1.publish(finalized_source);
    let finalized_target = store2
        .load()
        .as_ref()
        .clone()
        .finalize_slot_migration(slot, LOCAL_WORKER_ID)
        .unwrap();
    store2.publish(finalized_target);

    let moved_to_node2 = format!("-MOVED {} 127.0.0.1:{}\r\n", slot, addr2.port());
    send_and_expect(&mut node1, &get_key, moved_to_node2.as_bytes()).await;
    send_and_expect(&mut node2, &get_key, b"$5\r\nvalue\r\n").await;

    let _ = shutdown1_tx.send(());
    let _ = shutdown2_tx.send(());
    server1.await.unwrap();
    server2.await.unwrap();
}

#[tokio::test]
async fn detected_live_slot_migration_runner_updates_server_redirections() {
    let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr1 = listener1.local_addr().unwrap();
    let listener2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = listener2.local_addr().unwrap();

    let key = b"detected-live-migrate-key".to_vec();
    let slot = redis_hash_slot(&key);

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", addr1.port());
    let (next1, node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            addr2.port(),
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1
        .set_slot_state(slot, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", addr2.port());
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            addr1.port(),
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .set_slot_state(slot, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let store1 = Arc::new(ClusterConfigStore::new(config1));
    let store2 = Arc::new(ClusterConfigStore::new(config2));
    let source_processor = Arc::new(RequestProcessor::new().unwrap());
    let target_processor = Arc::new(RequestProcessor::new().unwrap());

    let (shutdown1_tx, shutdown1_rx) = oneshot::channel::<()>();
    let (shutdown2_tx, shutdown2_rx) = oneshot::channel::<()>();

    let server1_metrics = Arc::new(ServerMetrics::default());
    let server1_cluster = Arc::clone(&store1);
    let server1_processor = Arc::clone(&source_processor);
    let server1 = tokio::spawn(async move {
        run_listener_with_shutdown_and_cluster_with_processor(
            listener1,
            1024,
            server1_metrics,
            async move {
                let _ = shutdown1_rx.await;
            },
            Some(server1_cluster),
            server1_processor,
        )
        .await
        .unwrap();
    });

    let server2_metrics = Arc::new(ServerMetrics::default());
    let server2_cluster = Arc::clone(&store2);
    let server2_processor = Arc::clone(&target_processor);
    let server2 = tokio::spawn(async move {
        run_listener_with_shutdown_and_cluster_with_processor(
            listener2,
            1024,
            server2_metrics,
            async move {
                let _ = shutdown2_rx.await;
            },
            Some(server2_cluster),
            server2_processor,
        )
        .await
        .unwrap();
    });

    let mut node1 = TcpStream::connect(addr1).await.unwrap();
    let mut node2 = TcpStream::connect(addr2).await.unwrap();
    let set_key = encode_resp_command(&[b"SET", &key, b"value"]);
    let get_key = encode_resp_command(&[b"GET", &key]);

    send_and_expect(&mut node1, &set_key, b"+OK\r\n").await;
    send_and_expect(&mut node1, &get_key, b"$5\r\nvalue\r\n").await;

    let moved_to_node1 = format!("-MOVED {} 127.0.0.1:{}\r\n", slot, addr1.port());
    send_and_expect(&mut node2, &get_key, moved_to_node1.as_bytes()).await;

    let migration_source = store1
        .load()
        .as_ref()
        .clone()
        .begin_slot_migration_to(slot, node2_id_in_1)
        .unwrap();
    store1.publish(migration_source);
    let migration_target = store2
        .load()
        .as_ref()
        .clone()
        .begin_slot_import_from(slot, node1_id_in_2)
        .unwrap();
    store2.publish(migration_target);

    let ask_to_node2 = format!("-ASK {} 127.0.0.1:{}\r\n", slot, addr2.port());
    send_and_expect(&mut node1, &get_key, ask_to_node2.as_bytes()).await;

    let report = run_detected_live_slot_migrations_until_complete(
        &store1,
        &store2,
        &source_processor,
        &target_processor,
        1,
        Duration::from_millis(1),
        std::future::pending::<()>(),
    )
    .await
    .unwrap();
    assert!(!report.interrupted);
    assert_eq!(
        report.slots,
        vec![LiveSlotMigrationSlotReport {
            slot,
            batches: 1,
            moved_keys: 1,
            finalized: true,
        }]
    );

    let moved_to_node2 = format!("-MOVED {} 127.0.0.1:{}\r\n", slot, addr2.port());
    send_and_expect(&mut node1, &get_key, moved_to_node2.as_bytes()).await;
    send_and_expect(&mut node2, &get_key, b"$5\r\nvalue\r\n").await;

    let _ = shutdown1_tx.send(());
    let _ = shutdown2_tx.send(());
    server1.await.unwrap();
    server2.await.unwrap();
}

#[tokio::test]
async fn execute_live_slot_migration_orchestrates_transfer_and_finalization() {
    let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr1 = listener1.local_addr().unwrap();
    let listener2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = listener2.local_addr().unwrap();

    let key = b"orchestrated-live-migrate-key".to_vec();
    let slot = redis_hash_slot(&key);

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", addr1.port());
    let (next1, _node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            addr2.port(),
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1
        .set_slot_state(slot, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", addr2.port());
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            addr1.port(),
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .set_slot_state(slot, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let store1 = Arc::new(ClusterConfigStore::new(config1));
    let store2 = Arc::new(ClusterConfigStore::new(config2));
    let source_processor = Arc::new(RequestProcessor::new().unwrap());
    let target_processor = Arc::new(RequestProcessor::new().unwrap());

    let (shutdown1_tx, shutdown1_rx) = oneshot::channel::<()>();
    let (shutdown2_tx, shutdown2_rx) = oneshot::channel::<()>();

    let server1_metrics = Arc::new(ServerMetrics::default());
    let server1_cluster = Arc::clone(&store1);
    let server1_processor = Arc::clone(&source_processor);
    let server1 = tokio::spawn(async move {
        run_listener_with_shutdown_and_cluster_with_processor(
            listener1,
            1024,
            server1_metrics,
            async move {
                let _ = shutdown1_rx.await;
            },
            Some(server1_cluster),
            server1_processor,
        )
        .await
        .unwrap();
    });

    let server2_metrics = Arc::new(ServerMetrics::default());
    let server2_cluster = Arc::clone(&store2);
    let server2_processor = Arc::clone(&target_processor);
    let server2 = tokio::spawn(async move {
        run_listener_with_shutdown_and_cluster_with_processor(
            listener2,
            1024,
            server2_metrics,
            async move {
                let _ = shutdown2_rx.await;
            },
            Some(server2_cluster),
            server2_processor,
        )
        .await
        .unwrap();
    });

    let mut node1 = TcpStream::connect(addr1).await.unwrap();
    let mut node2 = TcpStream::connect(addr2).await.unwrap();
    let set_key = encode_resp_command(&[b"SET", &key, b"v-orchestrated"]);
    let get_key = encode_resp_command(&[b"GET", &key]);

    send_and_expect(&mut node1, &set_key, b"+OK\r\n").await;
    send_and_expect(&mut node1, &get_key, b"$14\r\nv-orchestrated\r\n").await;

    let moved_to_node1 = format!("-MOVED {} 127.0.0.1:{}\r\n", slot, addr1.port());
    send_and_expect(&mut node2, &get_key, moved_to_node1.as_bytes()).await;

    let moved = execute_live_slot_migration(
        &store1,
        &store2,
        &source_processor,
        &target_processor,
        slot,
        16,
    )
    .unwrap();
    assert_eq!(moved, 1);

    let moved_to_node2 = format!("-MOVED {} 127.0.0.1:{}\r\n", slot, addr2.port());
    send_and_expect(&mut node1, &get_key, moved_to_node2.as_bytes()).await;
    send_and_expect(&mut node2, &get_key, b"$14\r\nv-orchestrated\r\n").await;

    let _ = shutdown1_tx.send(());
    let _ = shutdown2_tx.send(());
    server1.await.unwrap();
    server2.await.unwrap();
}

#[test]
fn execute_live_slot_migration_batches_until_slot_exhausted() {
    let key1 = b"{batch-slot}one".to_vec();
    let key2 = b"{batch-slot}two".to_vec();
    let key3 = b"{batch-slot}three".to_vec();
    let slot = redis_hash_slot(&key1);
    assert_eq!(slot, redis_hash_slot(&key2));
    assert_eq!(slot, redis_hash_slot(&key3));

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", 7001);
    let (next1, node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            7002,
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1
        .set_slot_state(slot, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", 7002);
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            7001,
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .set_slot_state(slot, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let store1 = ClusterConfigStore::new(config1);
    let store2 = ClusterConfigStore::new(config2);
    let source = RequestProcessor::new().unwrap();
    let target = RequestProcessor::new().unwrap();

    let set1 = encode_resp_command(&[b"SET", &key1, b"v1"]);
    assert_eq!(execute_processor_frame(&source, &set1), b"+OK\r\n");
    let set2 = encode_resp_command(&[b"SET", &key2, b"v2"]);
    assert_eq!(execute_processor_frame(&source, &set2), b"+OK\r\n");
    let set3 = encode_resp_command(&[b"SET", &key3, b"v3"]);
    assert_eq!(execute_processor_frame(&source, &set3), b"+OK\r\n");

    let moved = execute_live_slot_migration(&store1, &store2, &source, &target, slot, 1)
        .expect("batched migration should complete");
    assert_eq!(moved, 3);

    let get1 = encode_resp_command(&[b"GET", &key1]);
    let get2 = encode_resp_command(&[b"GET", &key2]);
    let get3 = encode_resp_command(&[b"GET", &key3]);
    assert_eq!(execute_processor_frame(&source, &get1), b"$-1\r\n");
    assert_eq!(execute_processor_frame(&source, &get2), b"$-1\r\n");
    assert_eq!(execute_processor_frame(&source, &get3), b"$-1\r\n");
    assert_eq!(execute_processor_frame(&target, &get1), b"$2\r\nv1\r\n");
    assert_eq!(execute_processor_frame(&target, &get2), b"$2\r\nv2\r\n");
    assert_eq!(execute_processor_frame(&target, &get3), b"$2\r\nv3\r\n");

    assert_eq!(
        store1.load().slot_assigned_owner(slot).unwrap(),
        node2_id_in_1
    );
    assert_eq!(
        store2.load().slot_assigned_owner(slot).unwrap(),
        LOCAL_WORKER_ID
    );
}

#[test]
fn execute_live_slot_migration_step_progresses_then_finalizes() {
    let key1 = b"{step-slot}one".to_vec();
    let key2 = b"{step-slot}two".to_vec();
    let slot = redis_hash_slot(&key1);
    assert_eq!(slot, redis_hash_slot(&key2));

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", 7101);
    let (next1, node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            7102,
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1
        .set_slot_state(slot, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", 7102);
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            7101,
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .set_slot_state(slot, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let store1 = ClusterConfigStore::new(config1);
    let store2 = ClusterConfigStore::new(config2);
    let source = RequestProcessor::new().unwrap();
    let target = RequestProcessor::new().unwrap();

    let set1 = encode_resp_command(&[b"SET", &key1, b"v1"]);
    assert_eq!(execute_processor_frame(&source, &set1), b"+OK\r\n");
    let set2 = encode_resp_command(&[b"SET", &key2, b"v2"]);
    assert_eq!(execute_processor_frame(&source, &set2), b"+OK\r\n");

    let step1 =
        execute_live_slot_migration_step(&store1, &store2, &source, &target, slot, 1).unwrap();
    assert_eq!(
        step1,
        LiveSlotMigrationStepOutcome {
            moved_keys: 1,
            finalized: false,
        }
    );
    assert_eq!(
        store1.load().slot_state(slot).unwrap(),
        SlotState::Migrating
    );
    assert_eq!(
        store1.load().slot_assigned_owner(slot).unwrap(),
        node2_id_in_1
    );
    assert_eq!(
        store2.load().slot_state(slot).unwrap(),
        SlotState::Importing
    );
    assert_eq!(
        store2.load().slot_assigned_owner(slot).unwrap(),
        node1_id_in_2
    );

    let step2 =
        execute_live_slot_migration_step(&store1, &store2, &source, &target, slot, 1).unwrap();
    assert_eq!(
        step2,
        LiveSlotMigrationStepOutcome {
            moved_keys: 1,
            finalized: true,
        }
    );
    assert_eq!(store1.load().slot_state(slot).unwrap(), SlotState::Stable);
    assert_eq!(
        store1.load().slot_assigned_owner(slot).unwrap(),
        node2_id_in_1
    );
    assert_eq!(store2.load().slot_state(slot).unwrap(), SlotState::Stable);
    assert_eq!(
        store2.load().slot_assigned_owner(slot).unwrap(),
        LOCAL_WORKER_ID
    );

    let get1 = encode_resp_command(&[b"GET", &key1]);
    let get2 = encode_resp_command(&[b"GET", &key2]);
    assert_eq!(execute_processor_frame(&source, &get1), b"$-1\r\n");
    assert_eq!(execute_processor_frame(&source, &get2), b"$-1\r\n");
    assert_eq!(execute_processor_frame(&target, &get1), b"$2\r\nv1\r\n");
    assert_eq!(execute_processor_frame(&target, &get2), b"$2\r\nv2\r\n");
}

#[test]
fn execute_live_slot_migration_step_with_zero_batch_is_noop() {
    let key = b"{zero-step}k".to_vec();
    let slot = redis_hash_slot(&key);

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", 7201);
    let (next1, _node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            7202,
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1
        .set_slot_state(slot, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", 7202);
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            7201,
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .set_slot_state(slot, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let store1 = ClusterConfigStore::new(config1);
    let store2 = ClusterConfigStore::new(config2);
    let source = RequestProcessor::new().unwrap();
    let target = RequestProcessor::new().unwrap();

    let set = encode_resp_command(&[b"SET", &key, b"value"]);
    assert_eq!(execute_processor_frame(&source, &set), b"+OK\r\n");

    let step =
        execute_live_slot_migration_step(&store1, &store2, &source, &target, slot, 0).unwrap();
    assert_eq!(
        step,
        LiveSlotMigrationStepOutcome {
            moved_keys: 0,
            finalized: false,
        }
    );
    assert_eq!(store1.load().slot_state(slot).unwrap(), SlotState::Stable);
    assert_eq!(
        store1.load().slot_assigned_owner(slot).unwrap(),
        LOCAL_WORKER_ID
    );
    assert_eq!(store2.load().slot_state(slot).unwrap(), SlotState::Stable);
    assert_eq!(
        store2.load().slot_assigned_owner(slot).unwrap(),
        node1_id_in_2
    );
    let get = encode_resp_command(&[b"GET", &key]);
    assert_eq!(execute_processor_frame(&source, &get), b"$5\r\nvalue\r\n");
    assert_eq!(execute_processor_frame(&target, &get), b"$-1\r\n");
}

#[tokio::test]
async fn run_live_slot_migration_until_complete_reports_batches() {
    let key1 = b"{run-slot}one".to_vec();
    let key2 = b"{run-slot}two".to_vec();
    let key3 = b"{run-slot}three".to_vec();
    let slot = redis_hash_slot(&key1);
    assert_eq!(slot, redis_hash_slot(&key2));
    assert_eq!(slot, redis_hash_slot(&key3));

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", 7301);
    let (next1, node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            7302,
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1
        .set_slot_state(slot, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", 7302);
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            7301,
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .set_slot_state(slot, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let store1 = ClusterConfigStore::new(config1);
    let store2 = ClusterConfigStore::new(config2);
    let source = RequestProcessor::new().unwrap();
    let target = RequestProcessor::new().unwrap();

    let set1 = encode_resp_command(&[b"SET", &key1, b"v1"]);
    let set2 = encode_resp_command(&[b"SET", &key2, b"v2"]);
    let set3 = encode_resp_command(&[b"SET", &key3, b"v3"]);
    assert_eq!(execute_processor_frame(&source, &set1), b"+OK\r\n");
    assert_eq!(execute_processor_frame(&source, &set2), b"+OK\r\n");
    assert_eq!(execute_processor_frame(&source, &set3), b"+OK\r\n");

    let report = run_live_slot_migration_until_complete(
        &store1,
        &store2,
        &source,
        &target,
        slot,
        1,
        Duration::from_millis(1),
        std::future::pending::<()>(),
    )
    .await
    .unwrap();
    assert_eq!(
        report,
        LiveSlotMigrationRunReport {
            batches: 3,
            moved_keys: 3,
            finalized: true,
        }
    );

    let get1 = encode_resp_command(&[b"GET", &key1]);
    let get2 = encode_resp_command(&[b"GET", &key2]);
    let get3 = encode_resp_command(&[b"GET", &key3]);
    assert_eq!(execute_processor_frame(&source, &get1), b"$-1\r\n");
    assert_eq!(execute_processor_frame(&source, &get2), b"$-1\r\n");
    assert_eq!(execute_processor_frame(&source, &get3), b"$-1\r\n");
    assert_eq!(execute_processor_frame(&target, &get1), b"$2\r\nv1\r\n");
    assert_eq!(execute_processor_frame(&target, &get2), b"$2\r\nv2\r\n");
    assert_eq!(execute_processor_frame(&target, &get3), b"$2\r\nv3\r\n");
    assert_eq!(
        store1.load().slot_assigned_owner(slot).unwrap(),
        node2_id_in_1
    );
    assert_eq!(
        store2.load().slot_assigned_owner(slot).unwrap(),
        LOCAL_WORKER_ID
    );
}

#[tokio::test]
async fn run_live_slot_migration_until_complete_honors_shutdown() {
    let key = b"{run-shutdown}k".to_vec();
    let slot = redis_hash_slot(&key);

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", 7401);
    let (next1, _node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            7402,
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1
        .set_slot_state(slot, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", 7402);
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            7401,
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .set_slot_state(slot, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let store1 = ClusterConfigStore::new(config1);
    let store2 = ClusterConfigStore::new(config2);
    let source = RequestProcessor::new().unwrap();
    let target = RequestProcessor::new().unwrap();

    let set = encode_resp_command(&[b"SET", &key, b"value"]);
    assert_eq!(execute_processor_frame(&source, &set), b"+OK\r\n");

    let report = run_live_slot_migration_until_complete(
        &store1,
        &store2,
        &source,
        &target,
        slot,
        1,
        Duration::from_millis(1),
        std::future::ready(()),
    )
    .await
    .unwrap();
    assert_eq!(
        report,
        LiveSlotMigrationRunReport {
            batches: 0,
            moved_keys: 0,
            finalized: false,
        }
    );
    let get = encode_resp_command(&[b"GET", &key]);
    assert_eq!(execute_processor_frame(&source, &get), b"$5\r\nvalue\r\n");
    assert_eq!(execute_processor_frame(&target, &get), b"$-1\r\n");
    assert_eq!(store1.load().slot_state(slot).unwrap(), SlotState::Stable);
    assert_eq!(
        store1.load().slot_assigned_owner(slot).unwrap(),
        LOCAL_WORKER_ID
    );
    assert_eq!(store2.load().slot_state(slot).unwrap(), SlotState::Stable);
    assert_eq!(
        store2.load().slot_assigned_owner(slot).unwrap(),
        node1_id_in_2
    );
}

#[tokio::test]
async fn run_live_slot_migrations_until_complete_round_robins_slots() {
    let key_a1 = b"{multi-a}one".to_vec();
    let key_a2 = b"{multi-a}two".to_vec();
    let slot_a = redis_hash_slot(&key_a1);
    let mut key_b1 = b"{multi-b}one".to_vec();
    while redis_hash_slot(&key_b1) == slot_a {
        key_b1.push(b'x');
    }
    let key_b2 = [key_b1.as_slice(), b"-two".as_slice()].concat();
    let slot_b = redis_hash_slot(&key_b1);
    assert_ne!(slot_a, slot_b);
    assert_eq!(slot_a, redis_hash_slot(&key_a2));
    assert_eq!(slot_b, redis_hash_slot(&key_b2));

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", 7501);
    let (next1, node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            7502,
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1
        .set_slot_state(slot_a, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap()
        .set_slot_state(slot_b, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", 7502);
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            7501,
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .set_slot_state(slot_a, node1_id_in_2, SlotState::Stable)
        .unwrap()
        .set_slot_state(slot_b, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let store1 = ClusterConfigStore::new(config1);
    let store2 = ClusterConfigStore::new(config2);
    let source = RequestProcessor::new().unwrap();
    let target = RequestProcessor::new().unwrap();

    let set_a1 = encode_resp_command(&[b"SET", &key_a1, b"a1"]);
    let set_a2 = encode_resp_command(&[b"SET", &key_a2, b"a2"]);
    let set_b1 = encode_resp_command(&[b"SET", &key_b1, b"b1"]);
    let set_b2 = encode_resp_command(&[b"SET", &key_b2, b"b2"]);
    assert_eq!(execute_processor_frame(&source, &set_a1), b"+OK\r\n");
    assert_eq!(execute_processor_frame(&source, &set_a2), b"+OK\r\n");
    assert_eq!(execute_processor_frame(&source, &set_b1), b"+OK\r\n");
    assert_eq!(execute_processor_frame(&source, &set_b2), b"+OK\r\n");

    let report = run_live_slot_migrations_until_complete(
        &store1,
        &store2,
        &source,
        &target,
        &[slot_a, slot_b],
        1,
        Duration::from_millis(1),
        std::future::pending::<()>(),
    )
    .await
    .unwrap();

    assert!(!report.interrupted);
    assert_eq!(report.slots.len(), 2);
    let report_a = report
        .slots
        .iter()
        .find(|slot_report| slot_report.slot == slot_a)
        .unwrap();
    let report_b = report
        .slots
        .iter()
        .find(|slot_report| slot_report.slot == slot_b)
        .unwrap();
    assert_eq!(
        *report_a,
        LiveSlotMigrationSlotReport {
            slot: slot_a,
            batches: 2,
            moved_keys: 2,
            finalized: true,
        }
    );
    assert_eq!(
        *report_b,
        LiveSlotMigrationSlotReport {
            slot: slot_b,
            batches: 2,
            moved_keys: 2,
            finalized: true,
        }
    );

    let get_a1 = encode_resp_command(&[b"GET", &key_a1]);
    let get_a2 = encode_resp_command(&[b"GET", &key_a2]);
    let get_b1 = encode_resp_command(&[b"GET", &key_b1]);
    let get_b2 = encode_resp_command(&[b"GET", &key_b2]);
    assert_eq!(execute_processor_frame(&source, &get_a1), b"$-1\r\n");
    assert_eq!(execute_processor_frame(&source, &get_a2), b"$-1\r\n");
    assert_eq!(execute_processor_frame(&source, &get_b1), b"$-1\r\n");
    assert_eq!(execute_processor_frame(&source, &get_b2), b"$-1\r\n");
    assert_eq!(execute_processor_frame(&target, &get_a1), b"$2\r\na1\r\n");
    assert_eq!(execute_processor_frame(&target, &get_a2), b"$2\r\na2\r\n");
    assert_eq!(execute_processor_frame(&target, &get_b1), b"$2\r\nb1\r\n");
    assert_eq!(execute_processor_frame(&target, &get_b2), b"$2\r\nb2\r\n");
    assert_eq!(
        store1.load().slot_assigned_owner(slot_a).unwrap(),
        node2_id_in_1
    );
    assert_eq!(
        store1.load().slot_assigned_owner(slot_b).unwrap(),
        node2_id_in_1
    );
    assert_eq!(
        store2.load().slot_assigned_owner(slot_a).unwrap(),
        LOCAL_WORKER_ID
    );
    assert_eq!(
        store2.load().slot_assigned_owner(slot_b).unwrap(),
        LOCAL_WORKER_ID
    );
}

#[tokio::test]
async fn run_live_slot_migrations_until_complete_honors_shutdown() {
    let key = b"{multi-shutdown}k".to_vec();
    let slot = redis_hash_slot(&key);

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", 7601);
    let (next1, _node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            7602,
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1
        .set_slot_state(slot, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", 7602);
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            7601,
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .set_slot_state(slot, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let store1 = ClusterConfigStore::new(config1);
    let store2 = ClusterConfigStore::new(config2);
    let source = RequestProcessor::new().unwrap();
    let target = RequestProcessor::new().unwrap();

    let set = encode_resp_command(&[b"SET", &key, b"value"]);
    assert_eq!(execute_processor_frame(&source, &set), b"+OK\r\n");

    let report = run_live_slot_migrations_until_complete(
        &store1,
        &store2,
        &source,
        &target,
        &[slot],
        1,
        Duration::from_millis(1),
        std::future::ready(()),
    )
    .await
    .unwrap();
    assert!(report.interrupted);
    assert_eq!(
        report.slots,
        vec![LiveSlotMigrationSlotReport {
            slot,
            batches: 0,
            moved_keys: 0,
            finalized: false,
        }]
    );
    let get = encode_resp_command(&[b"GET", &key]);
    assert_eq!(execute_processor_frame(&source, &get), b"$5\r\nvalue\r\n");
    assert_eq!(execute_processor_frame(&target, &get), b"$-1\r\n");
    assert_eq!(store1.load().slot_state(slot).unwrap(), SlotState::Stable);
    assert_eq!(
        store1.load().slot_assigned_owner(slot).unwrap(),
        LOCAL_WORKER_ID
    );
    assert_eq!(store2.load().slot_state(slot).unwrap(), SlotState::Stable);
    assert_eq!(
        store2.load().slot_assigned_owner(slot).unwrap(),
        node1_id_in_2
    );
}

#[tokio::test]
async fn run_live_slot_migrations_until_complete_deduplicates_slots() {
    let key = b"{multi-dedupe}k".to_vec();
    let slot = redis_hash_slot(&key);

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", 7701);
    let (next1, node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            7702,
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1
        .set_slot_state(slot, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", 7702);
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            7701,
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .set_slot_state(slot, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let store1 = ClusterConfigStore::new(config1);
    let store2 = ClusterConfigStore::new(config2);
    let source = RequestProcessor::new().unwrap();
    let target = RequestProcessor::new().unwrap();

    let set = encode_resp_command(&[b"SET", &key, b"value"]);
    assert_eq!(execute_processor_frame(&source, &set), b"+OK\r\n");

    let report = run_live_slot_migrations_until_complete(
        &store1,
        &store2,
        &source,
        &target,
        &[slot, slot],
        1,
        Duration::from_millis(1),
        std::future::pending::<()>(),
    )
    .await
    .unwrap();
    assert!(!report.interrupted);
    assert_eq!(
        report.slots,
        vec![LiveSlotMigrationSlotReport {
            slot,
            batches: 1,
            moved_keys: 1,
            finalized: true,
        }]
    );
    let get = encode_resp_command(&[b"GET", &key]);
    assert_eq!(execute_processor_frame(&source, &get), b"$-1\r\n");
    assert_eq!(execute_processor_frame(&target, &get), b"$5\r\nvalue\r\n");
    assert_eq!(
        store1.load().slot_assigned_owner(slot).unwrap(),
        node2_id_in_1
    );
    assert_eq!(
        store2.load().slot_assigned_owner(slot).unwrap(),
        LOCAL_WORKER_ID
    );
}

#[tokio::test]
async fn run_live_slot_migrations_until_complete_zero_batch_is_interrupted_noop() {
    let key = b"{multi-zero}k".to_vec();
    let slot = redis_hash_slot(&key);

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", 7801);
    let (next1, _node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            7802,
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1
        .set_slot_state(slot, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", 7802);
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            7801,
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .set_slot_state(slot, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let store1 = ClusterConfigStore::new(config1);
    let store2 = ClusterConfigStore::new(config2);
    let source = RequestProcessor::new().unwrap();
    let target = RequestProcessor::new().unwrap();

    let set = encode_resp_command(&[b"SET", &key, b"value"]);
    assert_eq!(execute_processor_frame(&source, &set), b"+OK\r\n");

    let report = run_live_slot_migrations_until_complete(
        &store1,
        &store2,
        &source,
        &target,
        &[slot],
        0,
        Duration::from_millis(1),
        std::future::pending::<()>(),
    )
    .await
    .unwrap();
    assert!(report.interrupted);
    assert_eq!(
        report.slots,
        vec![LiveSlotMigrationSlotReport {
            slot,
            batches: 0,
            moved_keys: 0,
            finalized: false,
        }]
    );
    let get = encode_resp_command(&[b"GET", &key]);
    assert_eq!(execute_processor_frame(&source, &get), b"$5\r\nvalue\r\n");
    assert_eq!(execute_processor_frame(&target, &get), b"$-1\r\n");
}

#[test]
fn detect_live_slot_migration_slots_intersects_migrating_and_importing() {
    let slot_a = 510u16;
    let slot_b = 511u16;
    let slot_c = 512u16;
    let slot_d = 513u16;

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", 7901);
    let (next1, node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            7902,
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1
        .begin_slot_migration_to(slot_a, node2_id_in_1)
        .unwrap()
        .begin_slot_migration_to(slot_b, node2_id_in_1)
        .unwrap()
        .set_slot_state(slot_c, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", 7902);
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            7901,
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .begin_slot_import_from(slot_a, node1_id_in_2)
        .unwrap()
        .set_slot_state(slot_b, node1_id_in_2, SlotState::Stable)
        .unwrap()
        .begin_slot_import_from(slot_d, node1_id_in_2)
        .unwrap();

    let store1 = ClusterConfigStore::new(config1);
    let store2 = ClusterConfigStore::new(config2);
    let slots = detect_live_slot_migration_slots(&store1, &store2).unwrap();
    assert_eq!(slots, vec![slot_a]);
}

#[tokio::test]
async fn run_detected_live_slot_migrations_until_complete_executes_detected_slots() {
    let key_a = b"{detected-a}k".to_vec();
    let key_b = b"{detected-b}k".to_vec();
    let key_c = b"{detected-c}k".to_vec();
    let slot_a = redis_hash_slot(&key_a);
    let slot_b = redis_hash_slot(&key_b);
    let slot_c = redis_hash_slot(&key_c);
    assert_ne!(slot_a, slot_b);
    assert_ne!(slot_a, slot_c);
    assert_ne!(slot_b, slot_c);

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", 8001);
    let (next1, node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            8002,
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1
        .begin_slot_migration_to(slot_a, node2_id_in_1)
        .unwrap()
        .begin_slot_migration_to(slot_b, node2_id_in_1)
        .unwrap()
        .set_slot_state(slot_c, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", 8002);
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            8001,
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .begin_slot_import_from(slot_a, node1_id_in_2)
        .unwrap()
        .begin_slot_import_from(slot_b, node1_id_in_2)
        .unwrap()
        .set_slot_state(slot_c, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let store1 = ClusterConfigStore::new(config1);
    let store2 = ClusterConfigStore::new(config2);
    let source = RequestProcessor::new().unwrap();
    let target = RequestProcessor::new().unwrap();

    assert_eq!(
        execute_processor_frame(&source, &encode_resp_command(&[b"SET", &key_a, b"va"])),
        b"+OK\r\n"
    );
    assert_eq!(
        execute_processor_frame(&source, &encode_resp_command(&[b"SET", &key_b, b"vb"])),
        b"+OK\r\n"
    );
    assert_eq!(
        execute_processor_frame(&source, &encode_resp_command(&[b"SET", &key_c, b"vc"])),
        b"+OK\r\n"
    );

    let report = run_detected_live_slot_migrations_until_complete(
        &store1,
        &store2,
        &source,
        &target,
        1,
        Duration::from_millis(1),
        std::future::pending::<()>(),
    )
    .await
    .unwrap();
    assert!(!report.interrupted);
    assert_eq!(report.slots.len(), 2);
    let migrated_slots: HashSet<u16> = report.slots.iter().map(|slot| slot.slot).collect();
    assert_eq!(migrated_slots, HashSet::from([slot_a, slot_b]));
    for slot_report in report.slots {
        assert_eq!(slot_report.batches, 1);
        assert_eq!(slot_report.moved_keys, 1);
        assert!(slot_report.finalized);
    }

    assert_eq!(
        execute_processor_frame(&source, &encode_resp_command(&[b"GET", &key_a])),
        b"$-1\r\n"
    );
    assert_eq!(
        execute_processor_frame(&source, &encode_resp_command(&[b"GET", &key_b])),
        b"$-1\r\n"
    );
    assert_eq!(
        execute_processor_frame(&target, &encode_resp_command(&[b"GET", &key_a])),
        b"$2\r\nva\r\n"
    );
    assert_eq!(
        execute_processor_frame(&target, &encode_resp_command(&[b"GET", &key_b])),
        b"$2\r\nvb\r\n"
    );
    assert_eq!(
        execute_processor_frame(&source, &encode_resp_command(&[b"GET", &key_c])),
        b"$2\r\nvc\r\n"
    );
    assert_eq!(
        execute_processor_frame(&target, &encode_resp_command(&[b"GET", &key_c])),
        b"$-1\r\n"
    );

    for slot in [slot_a, slot_b] {
        assert_eq!(store1.load().slot_state(slot).unwrap(), SlotState::Stable);
        assert_eq!(store2.load().slot_state(slot).unwrap(), SlotState::Stable);
        assert_eq!(
            store1.load().slot_assigned_owner(slot).unwrap(),
            node2_id_in_1
        );
        assert_eq!(
            store2.load().slot_assigned_owner(slot).unwrap(),
            LOCAL_WORKER_ID
        );
    }
    assert_eq!(
        store1.load().slot_assigned_owner(slot_c).unwrap(),
        LOCAL_WORKER_ID
    );
    assert_eq!(
        store2.load().slot_assigned_owner(slot_c).unwrap(),
        node1_id_in_2
    );
}

#[tokio::test]
async fn run_detected_live_slot_migrations_until_complete_noop_when_no_detected_slots() {
    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", 8101);
    let (next1, _node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            8102,
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1
        .set_slot_state(901, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", 8102);
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            8101,
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .set_slot_state(901, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let store1 = ClusterConfigStore::new(config1);
    let store2 = ClusterConfigStore::new(config2);
    let source = RequestProcessor::new().unwrap();
    let target = RequestProcessor::new().unwrap();

    let report = run_detected_live_slot_migrations_until_complete(
        &store1,
        &store2,
        &source,
        &target,
        1,
        Duration::from_millis(1),
        std::future::pending::<()>(),
    )
    .await
    .unwrap();
    assert!(!report.interrupted);
    assert!(report.slots.is_empty());
    assert_eq!(
        store1.load().slot_assigned_owner(901).unwrap(),
        LOCAL_WORKER_ID
    );
    assert_eq!(
        store2.load().slot_assigned_owner(901).unwrap(),
        node1_id_in_2
    );
}

#[tokio::test]
async fn run_detected_live_slot_migrations_until_shutdown_runs_multiple_detection_cycles() {
    let key_a = b"{detected-cycle-a}k".to_vec();
    let key_b = b"{detected-cycle-b}k".to_vec();
    let slot_a = redis_hash_slot(&key_a);
    let slot_b = redis_hash_slot(&key_b);
    assert_ne!(slot_a, slot_b);

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", 8201);
    let (next1, node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            8202,
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1
        .set_slot_state(slot_a, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap()
        .set_slot_state(slot_b, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", 8202);
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            8201,
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .set_slot_state(slot_a, node1_id_in_2, SlotState::Stable)
        .unwrap()
        .set_slot_state(slot_b, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let store1 = Arc::new(ClusterConfigStore::new(config1));
    let store2 = Arc::new(ClusterConfigStore::new(config2));
    let source = Arc::new(RequestProcessor::new().unwrap());
    let target = Arc::new(RequestProcessor::new().unwrap());

    assert_eq!(
        execute_processor_frame(&source, &encode_resp_command(&[b"SET", &key_a, b"va"])),
        b"+OK\r\n"
    );

    let first_source = store1
        .load()
        .as_ref()
        .clone()
        .begin_slot_migration_to(slot_a, node2_id_in_1)
        .unwrap();
    store1.publish(first_source);
    let first_target = store2
        .load()
        .as_ref()
        .clone()
        .begin_slot_import_from(slot_a, node1_id_in_2)
        .unwrap();
    store2.publish(first_target);

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let orchestrator_store1 = Arc::clone(&store1);
    let orchestrator_store2 = Arc::clone(&store2);
    let orchestrator_source = Arc::clone(&source);
    let orchestrator_target = Arc::clone(&target);
    let orchestrator = tokio::spawn(async move {
        let get_a = encode_resp_command(&[b"GET", &key_a]);
        wait_until(
            || {
                execute_processor_frame(&orchestrator_source, &get_a) == b"$-1\r\n"
                    && execute_processor_frame(&orchestrator_target, &get_a) == b"$2\r\nva\r\n"
            },
            Duration::from_secs(1),
        )
        .await;
        assert_eq!(
            execute_processor_frame(&orchestrator_source, &get_a),
            b"$-1\r\n"
        );
        assert_eq!(
            execute_processor_frame(&orchestrator_target, &get_a),
            b"$2\r\nva\r\n"
        );

        assert_eq!(
            execute_processor_frame(
                &orchestrator_source,
                &encode_resp_command(&[b"SET", &key_b, b"vb"])
            ),
            b"+OK\r\n"
        );
        let second_source = orchestrator_store1
            .load()
            .as_ref()
            .clone()
            .begin_slot_migration_to(slot_b, node2_id_in_1)
            .unwrap();
        orchestrator_store1.publish(second_source);
        let second_target = orchestrator_store2
            .load()
            .as_ref()
            .clone()
            .begin_slot_import_from(slot_b, node1_id_in_2)
            .unwrap();
        orchestrator_store2.publish(second_target);

        let get_b = encode_resp_command(&[b"GET", &key_b]);
        wait_until(
            || {
                execute_processor_frame(&orchestrator_source, &get_b) == b"$-1\r\n"
                    && execute_processor_frame(&orchestrator_target, &get_b) == b"$2\r\nvb\r\n"
            },
            Duration::from_secs(1),
        )
        .await;
        assert_eq!(
            execute_processor_frame(&orchestrator_source, &get_b),
            b"$-1\r\n"
        );
        assert_eq!(
            execute_processor_frame(&orchestrator_target, &get_b),
            b"$2\r\nvb\r\n"
        );
        let _ = shutdown_tx.send(());
    });

    let reports = run_detected_live_slot_migrations_until_shutdown(
        &store1,
        &store2,
        &source,
        &target,
        1,
        Duration::from_millis(1),
        Duration::from_millis(1),
        async move {
            let _ = shutdown_rx.await;
        },
    )
    .await
    .unwrap();
    orchestrator.await.unwrap();

    assert_eq!(reports.len(), 2);
    assert!(reports.iter().all(|report| !report.interrupted));
    let migrated_slots: HashSet<u16> = reports
        .iter()
        .flat_map(|report| report.slots.iter().map(|slot| slot.slot))
        .collect();
    assert_eq!(migrated_slots, HashSet::from([slot_a, slot_b]));
}

#[tokio::test]
async fn run_detected_live_slot_migrations_until_shutdown_honors_shutdown_without_work() {
    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", 8301);
    let (next1, _node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            8302,
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1
        .set_slot_state(1001, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", 8302);
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            8301,
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .set_slot_state(1001, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let store1 = ClusterConfigStore::new(config1);
    let store2 = ClusterConfigStore::new(config2);
    let source = RequestProcessor::new().unwrap();
    let target = RequestProcessor::new().unwrap();

    let reports = run_detected_live_slot_migrations_until_shutdown(
        &store1,
        &store2,
        &source,
        &target,
        1,
        Duration::from_millis(1),
        Duration::from_millis(10),
        tokio::time::sleep(Duration::from_millis(5)),
    )
    .await
    .unwrap();
    assert!(reports.is_empty());
}

#[test]
fn owner_routed_shard_selection_handles_single_and_multi_key_commands() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 64];

    let get_frame = encode_resp_command(&[b"GET", b"my-key"]);
    let meta = parse_resp_command_arg_slices(&get_frame, &mut args).unwrap();
    let command = unsafe { dispatch_from_arg_slices(&args[..meta.argument_count]) };
    let routed_shard =
        owner_routed_shard_for_command(&processor, &args[..meta.argument_count], command);
    assert_eq!(
        routed_shard,
        Some(processor.string_store_shard_index(b"my-key"))
    );

    let del_multi_frame = encode_resp_command(&[b"DEL", b"k1", b"k2"]);
    let del_meta = parse_resp_command_arg_slices(&del_multi_frame, &mut args).unwrap();
    let del_command = unsafe { dispatch_from_arg_slices(&args[..del_meta.argument_count]) };
    assert_eq!(
        owner_routed_shard_for_command(&processor, &args[..del_meta.argument_count], del_command),
        None
    );
}

#[test]
fn execute_frame_via_processor_matches_direct_execution() {
    let processor = RequestProcessor::new().unwrap();

    let set_frame = encode_resp_command(&[b"SET", b"k", b"v"]);
    let routed_set = execute_frame_via_processor(&processor, &set_frame).unwrap();
    let direct_set = execute_processor_frame(&processor, &set_frame);
    assert_eq!(routed_set, direct_set);

    let get_frame = encode_resp_command(&[b"GET", b"k"]);
    let routed_get = execute_frame_via_processor(&processor, &get_frame).unwrap();
    let direct_get = execute_processor_frame(&processor, &get_frame);
    assert_eq!(routed_get, direct_get);

    assert!(matches!(
        execute_frame_via_processor(&processor, b"*1\r\n$4\r\nPING"),
        Err(RoutedExecutionError::Protocol)
    ));
}

#[test]
fn execute_owned_args_via_processor_matches_direct_execution() {
    let processor = RequestProcessor::new().unwrap();

    let set_frame = encode_resp_command(&[b"SET", b"k", b"v"]);
    let set_owned_args = owned_args_from_frame(&set_frame);
    let routed_set = execute_owned_args_via_processor(&processor, &set_owned_args).unwrap();
    let direct_set = execute_processor_frame(&processor, &set_frame);
    assert_eq!(routed_set, direct_set);

    let get_frame = encode_resp_command(&[b"GET", b"k"]);
    let get_owned_args = owned_args_from_frame(&get_frame);
    let routed_get = execute_owned_args_via_processor(&processor, &get_owned_args).unwrap();
    let direct_get = execute_processor_frame(&processor, &get_frame);
    assert_eq!(routed_get, direct_get);

    assert!(matches!(
        execute_owned_args_via_processor(&processor, &[]),
        Err(RoutedExecutionError::Protocol)
    ));
}

#[test]
fn execute_owned_frame_args_via_processor_matches_direct_execution() {
    let processor = RequestProcessor::new().unwrap();

    let set_frame = encode_resp_command(&[b"SET", b"k", b"v"]);
    let set_owned_args = owned_frame_args_from_frame(&set_frame);
    let routed_set = execute_owned_frame_args_via_processor(&processor, &set_owned_args).unwrap();
    let direct_set = execute_processor_frame(&processor, &set_frame);
    assert_eq!(routed_set, direct_set);

    let get_frame = encode_resp_command(&[b"GET", b"k"]);
    let get_owned_args = owned_frame_args_from_frame(&get_frame);
    let routed_get = execute_owned_frame_args_via_processor(&processor, &get_owned_args).unwrap();
    let direct_get = execute_processor_frame(&processor, &get_frame);
    assert_eq!(routed_get, direct_get);
}

#[test]
fn capture_owned_frame_args_rejects_invalid_argument_views() {
    let frame = encode_resp_command(&[b"GET", b"k"]);
    assert!(matches!(
        capture_owned_frame_args(&frame, &[]),
        Err(RoutedExecutionError::Protocol)
    ));

    let foreign_arg = ArgSlice::from_slice(b"foreign").unwrap();
    assert!(matches!(
        capture_owned_frame_args(&frame, &[foreign_arg]),
        Err(RoutedExecutionError::Protocol)
    ));
}

async fn wait_until<P>(mut predicate: P, timeout: Duration)
where
    P: FnMut() -> bool,
{
    let deadline = Instant::now() + timeout;
    loop {
        if predicate() || Instant::now() >= deadline {
            break;
        }
        sleep(Duration::from_millis(10)).await;
    }
}

async fn send_and_expect(client: &mut TcpStream, request: &[u8], expected_response: &[u8]) {
    client.write_all(request).await.unwrap();

    let mut actual = vec![0u8; expected_response.len()];
    tokio::time::timeout(Duration::from_secs(1), client.read_exact(&mut actual))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(actual, expected_response);
}

async fn send_and_read_integer(client: &mut TcpStream, request: &[u8], timeout: Duration) -> i64 {
    client.write_all(request).await.unwrap();
    let line = read_resp_line_with_timeout(client, timeout).await;
    assert!(line.starts_with(b":"));
    std::str::from_utf8(&line[1..])
        .unwrap()
        .parse::<i64>()
        .unwrap()
}

async fn send_and_read_bulk_payload(
    client: &mut TcpStream,
    request: &[u8],
    timeout: Duration,
) -> Vec<u8> {
    client.write_all(request).await.unwrap();
    let header = read_resp_line_with_timeout(client, timeout).await;
    assert!(header.starts_with(b"$"));
    let payload_len = std::str::from_utf8(&header[1..])
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let mut payload = vec![0u8; payload_len + 2];
    tokio::time::timeout(timeout, client.read_exact(&mut payload))
        .await
        .unwrap()
        .unwrap();
    payload.truncate(payload_len);
    payload
}

async fn read_resp_line_with_timeout(stream: &mut TcpStream, timeout: Duration) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        tokio::time::timeout(timeout, stream.read_exact(&mut byte))
            .await
            .unwrap()
            .unwrap();
        out.push(byte[0]);
        if out.ends_with(b"\r\n") {
            out.truncate(out.len() - 2);
            return out;
        }
    }
}

async fn read_exact_with_timeout(
    stream: &mut TcpStream,
    expected_len: usize,
    timeout: Duration,
) -> Vec<u8> {
    let mut response = vec![0u8; expected_len];
    tokio::time::timeout(timeout, stream.read_exact(&mut response))
        .await
        .unwrap()
        .unwrap();
    response
}

async fn wait_for_blocked_clients(stream: &mut TcpStream, expected: u64, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let payload =
            send_and_read_bulk_payload(stream, b"*1\r\n$4\r\nINFO\r\n", Duration::from_secs(1))
                .await;
        if read_info_u64(&payload, "blocked_clients") == Some(expected) {
            return;
        }
        sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for blocked_clients={expected}");
}

fn read_info_u64(payload: &[u8], field: &str) -> Option<u64> {
    let text = std::str::from_utf8(payload).ok()?;
    let prefix = format!("{field}:");
    text.split("\r\n").find_map(|line| {
        line.strip_prefix(&prefix)
            .and_then(|value| value.parse::<u64>().ok())
    })
}

fn encode_resp_command(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
    for part in parts {
        out.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        out.extend_from_slice(part);
        out.extend_from_slice(b"\r\n");
    }
    out
}

fn execute_processor_frame(processor: &RequestProcessor, frame: &[u8]) -> Vec<u8> {
    let mut args = [ArgSlice::EMPTY; 64];
    let meta = parse_resp_command_arg_slices(frame, &mut args).unwrap();
    let mut response = Vec::new();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    response
}

fn owned_args_from_frame(frame: &[u8]) -> Vec<Vec<u8>> {
    let mut args = [ArgSlice::EMPTY; 64];
    let meta = parse_resp_command_arg_slices(frame, &mut args).unwrap();
    let mut owned = Vec::with_capacity(meta.argument_count);
    for arg in &args[..meta.argument_count] {
        owned.push(arg_slice_bytes(&arg).to_vec());
    }
    owned
}

fn owned_frame_args_from_frame(frame: &[u8]) -> crate::connection_owner_routing::OwnedFrameArgs {
    let mut args = [ArgSlice::EMPTY; 64];
    let meta = parse_resp_command_arg_slices(frame, &mut args).unwrap();
    capture_owned_frame_args(frame, &args[..meta.argument_count]).unwrap()
}

#[inline]
fn arg_slice_bytes(arg: &ArgSlice) -> &[u8] {
    // SAFETY: test helpers use ArgSlice values derived from `frame` slices
    // that remain alive for the conversion scope.
    unsafe { arg.as_slice() }
}
