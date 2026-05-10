use super::*;
use garnet_cluster::SlotNumber;

#[test]
fn parse_server_config_from_values_applies_valid_overrides() {
    let config = parse_server_config_from_values(Some("127.0.0.1:7001"), Some("4096")).unwrap();
    assert_eq!(
        config.bind_addr,
        "127.0.0.1:7001".parse::<SocketAddr>().unwrap()
    );
    assert_eq!(config.read_buffer_size, 4096);
}

#[test]
fn parse_server_config_from_values_rejects_invalid_bind_addr() {
    let error = parse_server_config_from_values(Some("not-an-addr"), None).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("GARNET_BIND_ADDR"));
}

#[test]
fn parse_server_config_from_values_rejects_invalid_buffer_size() {
    let error = parse_server_config_from_values(None, Some("not-a-number")).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("GARNET_READ_BUFFER_SIZE"));
}

#[test]
fn parse_tokio_worker_threads_accepts_positive_value() {
    assert_eq!(parse_tokio_worker_threads(Some("4")).unwrap(), Some(4));
    assert_eq!(parse_tokio_worker_threads(Some(" 2 ")).unwrap(), Some(2));
    assert_eq!(parse_tokio_worker_threads(None).unwrap(), None);
}

#[test]
fn parse_tokio_worker_threads_rejects_invalid_or_zero_value() {
    let error = parse_tokio_worker_threads(Some("0")).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("TOKIO_WORKER_THREADS"));
    assert!(error.to_string().contains("must be >= 1"));

    let error = parse_tokio_worker_threads(Some("many")).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("TOKIO_WORKER_THREADS"));
}

#[test]
fn parse_server_launch_config_defaults_to_single_default_bind_addr() {
    let config =
        parse_server_launch_config_from_values(None, None, None, None, None, None, None, None)
            .unwrap();
    assert_eq!(config.bind_addrs.len(), 1);
    assert_eq!(config.bind_addrs[0], ServerConfig::default().bind_addr);
    assert!(!config.multi_port_cluster_mode);
    assert_eq!(config.multi_port_slot_policy, SlotOwnershipPolicy::Modulo);
    assert_eq!(
        config.read_buffer_size,
        ServerConfig::default().read_buffer_size
    );
}

#[test]
fn parse_server_launch_config_parses_multiple_bind_addrs() {
    let config = parse_server_launch_config_from_values(
        Some("127.0.0.1:7001"),
        Some("127.0.0.1:7101,127.0.0.1:7102"),
        Some("4"),
        Some("true"),
        Some("contiguous"),
        None,
        None,
        Some("2048"),
    )
    .unwrap();
    assert_eq!(config.bind_addrs.len(), 2);
    assert_eq!(
        config.bind_addrs,
        vec![
            "127.0.0.1:7101".parse::<SocketAddr>().unwrap(),
            "127.0.0.1:7102".parse::<SocketAddr>().unwrap()
        ]
    );
    assert!(config.multi_port_cluster_mode);
    assert_eq!(
        config.multi_port_slot_policy,
        SlotOwnershipPolicy::Contiguous
    );
    assert_eq!(config.read_buffer_size, 2048);
}

#[test]
fn parse_server_launch_config_rejects_duplicate_bind_addrs() {
    let error = parse_server_launch_config_from_values(
        None,
        Some("127.0.0.1:7101,127.0.0.1:7101"),
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("duplicate address"));
}

#[test]
fn parse_server_launch_config_rejects_empty_bind_addrs_segment() {
    let error = parse_server_launch_config_from_values(
        None,
        Some("127.0.0.1:7101, ,127.0.0.1:7102"),
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("empty address segment"));
}

#[test]
fn parse_server_launch_config_expands_owner_node_count_from_bind_addr() {
    let config = parse_server_launch_config_from_values(
        Some("127.0.0.1:7300"),
        None,
        Some("4"),
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    assert_eq!(
        config.bind_addrs,
        vec![
            "127.0.0.1:7300".parse::<SocketAddr>().unwrap(),
            "127.0.0.1:7301".parse::<SocketAddr>().unwrap(),
            "127.0.0.1:7302".parse::<SocketAddr>().unwrap(),
            "127.0.0.1:7303".parse::<SocketAddr>().unwrap(),
        ]
    );
}

#[test]
fn parse_server_launch_config_rejects_invalid_owner_node_count() {
    let error = parse_server_launch_config_from_values(
        None,
        None,
        Some("not-a-number"),
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("GARNET_OWNER_NODE_COUNT"));
}

#[test]
fn parse_server_launch_config_rejects_zero_owner_node_count() {
    let error =
        parse_server_launch_config_from_values(None, None, Some("0"), None, None, None, None, None)
            .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("must be >= 1"));
}

#[test]
fn parse_server_launch_config_rejects_owner_node_count_port_overflow() {
    let error = parse_server_launch_config_from_values(
        Some("127.0.0.1:65535"),
        None,
        Some("2"),
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("exceeds 65535"));
}

#[test]
fn parse_server_launch_config_bind_addrs_takes_priority_over_owner_node_count() {
    let config = parse_server_launch_config_from_values(
        Some("127.0.0.1:7400"),
        Some("127.0.0.1:7501,127.0.0.1:7502"),
        Some("8"),
        Some("false"),
        Some("modulo"),
        None,
        None,
        None,
    )
    .unwrap();
    assert_eq!(
        config.bind_addrs,
        vec![
            "127.0.0.1:7501".parse::<SocketAddr>().unwrap(),
            "127.0.0.1:7502".parse::<SocketAddr>().unwrap(),
        ]
    );
}

#[test]
fn parse_server_launch_config_rejects_invalid_cluster_mode_flag() {
    let error = parse_server_launch_config_from_values(
        None,
        None,
        None,
        Some("maybe"),
        None,
        None,
        None,
        None,
    )
    .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("GARNET_MULTI_PORT_CLUSTER_MODE"));
}

#[test]
fn parse_server_launch_config_rejects_invalid_slot_policy() {
    let error = parse_server_launch_config_from_values(
        None,
        None,
        None,
        None,
        Some("random"),
        None,
        None,
        None,
    )
    .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("GARNET_MULTI_PORT_SLOT_POLICY"));
}

#[test]
fn parse_server_launch_config_enables_thread_pinning_from_flag() {
    let config = parse_server_launch_config_from_values(
        Some("127.0.0.1:7600"),
        None,
        Some("2"),
        None,
        None,
        Some("true"),
        None,
        None,
    )
    .unwrap();
    assert!(config.owner_thread_pinning.enabled);
    assert!(config.owner_thread_pinning.cpu_set.is_empty());
}

#[test]
fn parse_server_launch_config_enables_thread_pinning_from_cpu_set() {
    let config = parse_server_launch_config_from_values(
        Some("127.0.0.1:7600"),
        None,
        Some("2"),
        None,
        None,
        None,
        Some("0,2"),
        None,
    )
    .unwrap();
    assert!(config.owner_thread_pinning.enabled);
    assert_eq!(config.owner_thread_pinning.cpu_set, vec![0, 2]);
}

#[test]
fn parse_server_launch_config_rejects_invalid_cpu_set() {
    let error = parse_server_launch_config_from_values(
        None,
        None,
        None,
        None,
        None,
        None,
        Some("0, ,2"),
        None,
    )
    .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("GARNET_OWNER_THREAD_CPU_SET"));
}

#[test]
fn resolve_core_assignments_round_robins_available_cores() {
    let pinning = ThreadPinningConfig {
        enabled: true,
        cpu_set: Vec::new(),
    };
    let assignments = resolve_core_assignments_from_available(&[0, 1], 5, &pinning).unwrap();
    assert_eq!(
        assignments,
        vec![Some(0), Some(1), Some(0), Some(1), Some(0)]
    );
}

#[test]
fn resolve_core_assignments_uses_requested_cpu_set() {
    let pinning = ThreadPinningConfig {
        enabled: true,
        cpu_set: vec![3, 1],
    };
    let assignments = resolve_core_assignments_from_available(&[0, 1, 2, 3], 4, &pinning).unwrap();
    assert_eq!(assignments, vec![Some(3), Some(1), Some(3), Some(1)]);
}

#[test]
fn resolve_core_assignments_rejects_unavailable_requested_cpu() {
    let pinning = ThreadPinningConfig {
        enabled: true,
        cpu_set: vec![4],
    };
    let error = resolve_core_assignments_from_available(&[0, 1, 2], 2, &pinning).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("not available"));
}

#[test]
fn parse_startup_config_overrides_accepts_valid_persistence_and_acl_values() {
    let overrides = parse_startup_config_overrides_from_values(
        Some("./data"),
        Some("custom.rdb"),
        Some("yes"),
        Some("always"),
        Some("append.aof"),
        Some("users.acl"),
        Some("alice on >alice +@all ~*\nbob on nopass +acl"),
    )
    .unwrap();
    assert_eq!(overrides.dir, Some(std::path::PathBuf::from("./data")));
    assert_eq!(overrides.dbfilename.as_deref(), Some("custom.rdb"));
    assert_eq!(overrides.appendonly, Some(true));
    assert_eq!(overrides.appendfsync.as_deref(), Some("always"));
    assert_eq!(overrides.appendfilename.as_deref(), Some("append.aof"));
    assert_eq!(
        overrides.aclfile,
        Some(std::path::PathBuf::from("users.acl"))
    );
    assert_eq!(
        overrides.users,
        vec![
            "alice on >alice +@all ~*".to_string(),
            "bob on nopass +acl".to_string()
        ]
    );
}

#[test]
fn parse_startup_config_overrides_rejects_invalid_appendfilename_and_appendfsync() {
    let appendfilename_error = parse_startup_config_overrides_from_values(
        None,
        None,
        None,
        None,
        Some("nested/append.aof"),
        None,
        None,
    )
    .unwrap_err();
    assert_eq!(
        appendfilename_error.kind(),
        std::io::ErrorKind::InvalidInput
    );
    assert!(
        appendfilename_error
            .to_string()
            .contains("GARNET_APPENDFILENAME")
    );

    let appendfsync_error = parse_startup_config_overrides_from_values(
        None,
        None,
        None,
        Some("sometimes"),
        None,
        None,
        None,
    )
    .unwrap_err();
    assert_eq!(appendfsync_error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(appendfsync_error.to_string().contains("GARNET_APPENDFSYNC"));
}

#[test]
fn validate_server_launch_config_rejects_multi_port_with_startup_overrides() {
    let mut launch = parse_server_launch_config_from_values(
        Some("127.0.0.1:7600"),
        None,
        Some("2"),
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    launch.startup_config_overrides = parse_startup_config_overrides_from_values(
        Some("./data"),
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();

    let error = validate_server_launch_config(&launch).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("multi-port"));
}

#[test]
fn build_multi_port_cluster_stores_assigns_slot_owners_by_modulo() {
    let bind_addrs = vec![
        "127.0.0.1:8101".parse::<SocketAddr>().unwrap(),
        "127.0.0.1:8102".parse::<SocketAddr>().unwrap(),
        "127.0.0.1:8103".parse::<SocketAddr>().unwrap(),
    ];
    let stores = build_multi_port_cluster_stores(&bind_addrs, SlotOwnershipPolicy::Modulo).unwrap();
    assert_eq!(stores.len(), 3);

    let local0 = stores[0].load();
    let local1 = stores[1].load();

    assert_eq!(local0.local_worker().unwrap().endpoint(), "127.0.0.1:8101");
    assert_eq!(local1.local_worker().unwrap().endpoint(), "127.0.0.1:8102");

    let moved_from_0_for_slot_1 = local0
        .redirection_error_for_slot(SlotNumber::new(1))
        .unwrap();
    assert_eq!(
        moved_from_0_for_slot_1.as_deref(),
        Some("MOVED 1 127.0.0.1:8102")
    );

    let moved_from_1_for_slot_2 = local1
        .redirection_error_for_slot(SlotNumber::new(2))
        .unwrap();
    assert_eq!(
        moved_from_1_for_slot_2.as_deref(),
        Some("MOVED 2 127.0.0.1:8103")
    );
}

#[test]
fn slot_owner_index_contiguous_assigns_balanced_ranges() {
    let node_count = 3;
    assert_eq!(
        slot_owner_index(0, node_count, SlotOwnershipPolicy::Contiguous),
        0
    );
    assert_eq!(
        slot_owner_index(5461, node_count, SlotOwnershipPolicy::Contiguous),
        0
    );
    assert_eq!(
        slot_owner_index(5462, node_count, SlotOwnershipPolicy::Contiguous),
        1
    );
    assert_eq!(
        slot_owner_index(10922, node_count, SlotOwnershipPolicy::Contiguous),
        1
    );
    assert_eq!(
        slot_owner_index(10923, node_count, SlotOwnershipPolicy::Contiguous),
        2
    );
    assert_eq!(
        slot_owner_index(16383, node_count, SlotOwnershipPolicy::Contiguous),
        2
    );
}

#[test]
fn build_multi_port_cluster_stores_assigns_slot_owners_by_contiguous_ranges() {
    let bind_addrs = vec![
        "127.0.0.1:8201".parse::<SocketAddr>().unwrap(),
        "127.0.0.1:8202".parse::<SocketAddr>().unwrap(),
        "127.0.0.1:8203".parse::<SocketAddr>().unwrap(),
    ];
    let stores =
        build_multi_port_cluster_stores(&bind_addrs, SlotOwnershipPolicy::Contiguous).unwrap();
    let local0 = stores[0].load();

    let moved_at_start_of_worker1 = local0
        .redirection_error_for_slot(SlotNumber::new(5462))
        .unwrap();
    assert_eq!(
        moved_at_start_of_worker1.as_deref(),
        Some("MOVED 5462 127.0.0.1:8202")
    );

    let moved_at_start_of_worker2 = local0
        .redirection_error_for_slot(SlotNumber::new(10923))
        .unwrap();
    assert_eq!(
        moved_at_start_of_worker2.as_deref(),
        Some("MOVED 10923 127.0.0.1:8203")
    );
}
