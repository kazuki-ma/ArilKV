use garnet_server::{run, ServerConfig, ServerMetrics};
use std::net::SocketAddr;
use std::sync::Arc;

fn parse_server_config_from_values(
    bind_addr: Option<&str>,
    read_buffer_size: Option<&str>,
) -> std::io::Result<ServerConfig> {
    let mut config = ServerConfig::default();
    if let Some(bind_addr_raw) = bind_addr {
        config.bind_addr = bind_addr_raw.parse::<SocketAddr>().map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("invalid GARNET_BIND_ADDR `{bind_addr_raw}`: {error}"),
            )
        })?;
    }
    if let Some(read_buffer_size_raw) = read_buffer_size {
        config.read_buffer_size = read_buffer_size_raw.parse::<usize>().map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("invalid GARNET_READ_BUFFER_SIZE `{read_buffer_size_raw}`: {error}"),
            )
        })?;
    }
    Ok(config)
}

fn parse_server_config_from_env() -> std::io::Result<ServerConfig> {
    parse_server_config_from_values(
        std::env::var("GARNET_BIND_ADDR").ok().as_deref(),
        std::env::var("GARNET_READ_BUFFER_SIZE").ok().as_deref(),
    )
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> std::io::Result<()> {
    let config = parse_server_config_from_env()?;
    let metrics = Arc::new(ServerMetrics::default());
    run(config, metrics).await
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
