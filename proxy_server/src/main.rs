use aerospike::{AuthMode, Client, ClientPolicy};
use clap::Parser;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use as_proxy_server::auth::Authenticator;
use as_proxy_server::config::AppConfig;
use as_proxy_server::flight_service::ProxyFlightService;
use as_proxy_server::metrics::{ProxyMetrics, serve_observability};
use as_proxy_server::raft::{MetadataCommand, NodeStatusMeta, RaftNode, now_ms};
use as_proxy_server::writer::{MppWriteEngine, RealAerospikeSink};

#[derive(Debug, Parser)]
#[command(
    name = "as_proxy_server",
    about = "High-performance Arrow Flight to Aerospike 8+ MPP Proxy Server with Raft Metadata Cluster"
)]
struct CliArgs {
    /// Path to TOML configuration file
    #[arg(short, long, env = "AS_PROXY_CONFIG")]
    config: Option<PathBuf>,

    /// Override listen address (e.g. 0.0.0.0:8815)
    #[arg(long, env = "AS_PROXY_LISTEN_ADDR")]
    listen_addr: Option<String>,

    /// Override Raft listen address (e.g. 0.0.0.0:8816)
    #[arg(long, env = "AS_PROXY_RAFT_ADDR")]
    raft_addr: Option<String>,

    /// Override Raft node ID
    #[arg(long, env = "AS_PROXY_NODE_ID")]
    node_id: Option<u64>,

    /// Override Aerospike seed hosts (e.g. 127.0.0.1:3000)
    #[arg(long, env = "AEROSPIKE_HOSTS")]
    aerospike_hosts: Option<String>,
}

fn apply_cli_overrides(cfg: &mut AppConfig, args: CliArgs) {
    if let Some(addr) = args.listen_addr {
        cfg.server.listen_addr = addr;
    }
    if let Some(r_addr) = args.raft_addr {
        cfg.raft.listen_addr = r_addr;
    }
    if let Some(nid) = args.node_id {
        cfg.raft.node_id = nid;
    }
    if let Some(hosts) = args.aerospike_hosts {
        cfg.aerospike.hosts = hosts;
    }
}

fn load_and_apply_config(args: CliArgs) -> Result<AppConfig, Box<dyn std::error::Error>> {
    let mut cfg = match args.config {
        Some(ref path) => AppConfig::from_file(path)?,
        None => AppConfig::default(),
    };
    apply_cli_overrides(&mut cfg, args);
    Ok(cfg)
}

fn build_client_policy(cfg: &AppConfig) -> ClientPolicy {
    let mut client_policy = ClientPolicy::default();
    client_policy.auth_mode = match cfg
        .aerospike
        .user
        .as_deref()
        .map(str::trim)
        .filter(|u| !u.is_empty())
    {
        Some(user) => AuthMode::Internal(
            user.to_string(),
            cfg.aerospike.password.clone().unwrap_or_default(),
        ),
        None => AuthMode::None,
    };
    client_policy.timeout = cfg.aerospike.timeout_ms;
    client_policy.connect_timeout = cfg.aerospike.connect_timeout_ms;
    client_policy.min_conns_per_node = cfg.aerospike.min_conns_per_node;
    client_policy.max_conns_per_node = cfg.aerospike.max_conns_per_node;
    client_policy.conn_pools_per_node = cfg.aerospike.conn_pools_per_node;
    client_policy.buffer_reclaim_threshold = 262_144;
    client_policy
}

fn check_security_warnings(cfg: &AppConfig) -> Vec<&'static str> {
    let mut warnings = Vec::new();
    if cfg.auth.uses_default_secrets() {
        warnings.push(
            "Security warning: auth.enabled = true with default/example password or token_secret; change them before production use!",
        );
    }
    if cfg.auth.enabled && !cfg.tls.enabled {
        warnings.push(
            "Security warning: auth.enabled = true while tls.enabled = false; Basic credentials and Bearer tokens are transmitted without TLS encryption.",
        );
    }
    if cfg.raft.enabled
        && cfg.raft.peers.iter().any(|p| p.node_id != cfg.raft.node_id)
        && cfg
            .raft
            .cluster_secret
            .as_deref()
            .unwrap_or("")
            .trim()
            .is_empty()
    {
        warnings.push(
            "Security warning: multi-node Raft is enabled without raft.cluster_secret; configure raft.cluster_secret or isolate the Raft port via firewall.",
        );
    }
    warnings
}

fn build_node_status_meta(
    cfg: &AppConfig,
    metrics: &ProxyMetrics,
    aerospike_connected: bool,
) -> NodeStatusMeta {
    NodeStatusMeta {
        node_id: cfg.raft.node_id,
        raft_addr: cfg.raft.listen_addr.clone(),
        flight_addr: cfg.server.listen_addr.clone(),
        active_streams: metrics.active_streams.load(Ordering::Relaxed),
        in_flight_records: metrics.in_flight_records.load(Ordering::Relaxed),
        total_records_written: metrics.records_written_total.load(Ordering::Relaxed),
        aerospike_connected,
        last_updated_ms: now_ms(),
    }
}

fn configure_server_builder(cfg: &AppConfig) -> Result<Server, Box<dyn std::error::Error>> {
    let mut server_builder = Server::builder()
        .tcp_nodelay(cfg.server.tcp_nodelay)
        .initial_stream_window_size(cfg.server.http2_stream_window_size_mb * 1024 * 1024)
        .initial_connection_window_size(cfg.server.http2_connection_window_size_mb * 1024 * 1024);

    if cfg.tls.enabled {
        let cert_path = cfg
            .tls
            .cert_path
            .as_ref()
            .ok_or("tls.cert_path is required when tls.enabled = true")?;
        let key_path = cfg
            .tls
            .key_path
            .as_ref()
            .ok_or("tls.key_path is required when tls.enabled = true")?;

        let cert_pem = fs::read(cert_path)?;
        let key_pem = fs::read(key_path)?;
        let identity = Identity::from_pem(cert_pem, key_pem);

        let mut tls_config = ServerTlsConfig::new().identity(identity);
        if let Some(ref ca_path) = cfg.tls.client_ca_path {
            let ca_pem = fs::read(ca_path)?;
            tls_config = tls_config.client_ca_root(Certificate::from_pem(ca_pem));
        }

        server_builder = server_builder.tls_config(tls_config)?;
        info!("TLS enabled on Arrow Flight gRPC server");
    }

    Ok(server_builder)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cfg = load_and_apply_config(CliArgs::parse())?;
    for w in check_security_warnings(&cfg) {
        warn!("{w}");
    }

    info!(
        node_id = cfg.raft.node_id,
        listen_addr = %cfg.server.listen_addr,
        raft_addr = %cfg.raft.listen_addr,
        metrics_addr = %cfg.server.metrics_addr,
        aerospike_hosts = %cfg.aerospike.hosts,
        tls_enabled = cfg.tls.enabled,
        auth_enabled = cfg.auth.enabled,
        compression_enabled = cfg.server.compression_enabled,
        compression = ?cfg.server.compression,
        compression_level = cfg.server.compression_level,
        use_batch_write = cfg.writer.use_batch_write,
        batch_write_size = cfg.writer.batch_write_size,
        max_in_flight_records = cfg.writer.max_in_flight_records,
        "Starting Arrow Flight -> Aerospike 8+ MPP Proxy Server"
    );

    let client_policy = build_client_policy(&cfg);
    let as_client = Arc::new(
        Client::new(&client_policy, &cfg.aerospike.hosts)
            .await
            .map_err(|e| {
                format!(
                    "Failed to connect to Aerospike at {}: {e}",
                    cfg.aerospike.hosts
                )
            })?,
    );
    info!("Connected to Aerospike cluster successfully");

    let metrics = ProxyMetrics::new();
    let sink = Arc::new(RealAerospikeSink::new(
        Arc::clone(&as_client),
        cfg.aerospike.timeout_ms,
    ));
    let write_engine = Arc::new(MppWriteEngine::new(
        sink,
        cfg.writer.clone(),
        cfg.retry.clone(),
        Arc::clone(&metrics),
    ));

    // Start Raft metadata service on its dedicated port
    let raft_node = if cfg.raft.enabled {
        let node = RaftNode::new(cfg.raft.clone());
        node.start().await?;

        let raft_for_hb = Arc::clone(&node);
        let metrics_for_hb = Arc::clone(&metrics);
        let engine_for_hb = Arc::clone(&write_engine);
        let cfg_for_hb = cfg.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(2));
            loop {
                interval.tick().await;
                let meta = build_node_status_meta(
                    &cfg_for_hb,
                    &metrics_for_hb,
                    engine_for_hb.is_connected(),
                );
                let _ = raft_for_hb
                    .propose(MetadataCommand::UpsertNodeStatus(meta))
                    .await;
            }
        });

        Some(node)
    } else {
        None
    };

    // Spawn HTTP observability server (/metrics, /healthz)
    let metrics_addr = cfg.server.metrics_addr.clone();
    let metrics_clone = Arc::clone(&metrics);
    let raft_for_obs = raft_node.clone();
    let engine_for_health = Arc::clone(&write_engine);
    tokio::spawn(async move {
        if let Err(e) = serve_observability(&metrics_addr, metrics_clone, raft_for_obs, move || {
            engine_for_health.is_connected()
        })
        .await
        {
            tracing::error!("Observability server failed: {e}");
        }
    });

    let authenticator = Arc::new(Authenticator::new(cfg.auth.clone()));
    let mut flight_service = ProxyFlightService::new(
        authenticator,
        write_engine,
        metrics,
        cfg.aerospike.max_bin_name_len,
    )
    .with_compression(cfg.server.compression_enabled, cfg.server.compression)
    .with_compression_level(cfg.server.compression_level);
    if let Some(raft) = raft_node {
        flight_service = flight_service.with_raft_node(raft);
    }

    let svc = flight_service.into_server(&cfg.server);

    let addr: SocketAddr = cfg.server.listen_addr.parse()?;
    let mut server_builder = configure_server_builder(&cfg)?;

    info!("Arrow Flight gRPC server listening on {addr}");
    server_builder.add_service(svc).serve(addr).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_args_parsing_and_helpers() {
        let args = CliArgs::try_parse_from([
            "as_proxy_server",
            "--listen-addr",
            "127.0.0.1:9915",
            "--raft-addr",
            "127.0.0.1:9916",
            "--node-id",
            "3",
            "--aerospike-hosts",
            "10.0.0.10:3000",
        ])
        .unwrap();

        let mut cfg = load_and_apply_config(args).unwrap();
        assert_eq!(cfg.server.listen_addr, "127.0.0.1:9915");
        assert_eq!(cfg.raft.listen_addr, "127.0.0.1:9916");
        assert_eq!(cfg.raft.node_id, 3);
        assert_eq!(cfg.aerospike.hosts, "10.0.0.10:3000");

        let policy = build_client_policy(&cfg);
        assert_eq!(policy.timeout, cfg.aerospike.timeout_ms);

        let metrics = ProxyMetrics::new();
        let meta = build_node_status_meta(&cfg, &metrics, true);
        assert_eq!(meta.node_id, 3);
        assert!(meta.aerospike_connected);

        assert!(configure_server_builder(&cfg).is_ok());

        // TLS enabled validation checks
        cfg.tls.enabled = true;
        assert!(configure_server_builder(&cfg).is_err());
        cfg.tls.cert_path = Some("/nonexistent/cert.pem".into());
        assert!(configure_server_builder(&cfg).is_err());
        cfg.tls.key_path = Some("/nonexistent/key.pem".into());
        assert!(configure_server_builder(&cfg).is_err());

        let cert_file = tempfile::NamedTempFile::new().unwrap();
        let key_file = tempfile::NamedTempFile::new().unwrap();
        let ca_file = tempfile::NamedTempFile::new().unwrap();
        fs::write(
            cert_file.path(),
            b"-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----\n",
        )
        .unwrap();
        fs::write(
            key_file.path(),
            b"-----BEGIN PRIVATE KEY-----\n-----END PRIVATE KEY-----\n",
        )
        .unwrap();
        fs::write(
            ca_file.path(),
            b"-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----\n",
        )
        .unwrap();

        cfg.tls.cert_path = Some(cert_file.path().to_string_lossy().to_string());
        cfg.tls.key_path = Some(key_file.path().to_string_lossy().to_string());
        cfg.tls.client_ca_path = Some(ca_file.path().to_string_lossy().to_string());
        let _ = configure_server_builder(&cfg);

        // Also test loading from --config file
        let cfg_file = tempfile::NamedTempFile::new().unwrap();
        fs::write(
            cfg_file.path(),
            "[server]\nlisten_addr = \"127.0.0.1:7715\"\n",
        )
        .unwrap();
        let args_from_file = CliArgs::try_parse_from([
            "as_proxy_server",
            "--config",
            &cfg_file.path().to_string_lossy(),
        ])
        .unwrap();
        let loaded = load_and_apply_config(args_from_file).unwrap();
        assert_eq!(loaded.server.listen_addr, "127.0.0.1:7715");

        // Security warnings & Aerospike AuthMode::Internal coverage
        let warnings_default = check_security_warnings(&loaded);
        assert!(warnings_default.len() >= 2);

        let mut hardened = loaded.clone();
        hardened.tls.enabled = true;
        hardened.auth.password = "custom_strong_pass_999".to_string();
        hardened.auth.token_secret = "custom_strong_hmac_secret_999".to_string();
        hardened.aerospike.user = Some("as_app".to_string());
        hardened.aerospike.password = Some("as_secret".to_string());
        assert!(check_security_warnings(&hardened).is_empty());

        let auth_policy = build_client_policy(&hardened);
        assert!(matches!(auth_policy.auth_mode, AuthMode::Internal(ref u, _) if u == "as_app"));
        let dbg_str = format!("{hardened:?}");
        assert!(!dbg_str.contains("custom_strong_pass_999"));
        assert!(!dbg_str.contains("custom_strong_hmac_secret_999"));
        assert!(!dbg_str.contains("as_secret"));
    }
}
