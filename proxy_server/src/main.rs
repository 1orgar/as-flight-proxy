use aerospike::{AuthMode, Client, ClientPolicy};
use arrow_flight::flight_service_server::FlightServiceServer;
use clap::Parser;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tracing::info;
use tracing_subscriber::EnvFilter;

use as_proxy_server::auth::Authenticator;
use as_proxy_server::config::AppConfig;
use as_proxy_server::flight_service::ProxyFlightService;
use as_proxy_server::metrics::{ProxyMetrics, serve_observability};
use as_proxy_server::writer::{MppWriteEngine, RealAerospikeSink};

#[derive(Debug, Parser)]
#[command(
    name = "as_proxy_server",
    about = "High-performance Arrow Flight to Aerospike 8+ MPP Proxy Server"
)]
struct CliArgs {
    /// Path to TOML configuration file
    #[arg(short, long, env = "AS_PROXY_CONFIG")]
    config: Option<PathBuf>,

    /// Override listen address (e.g. 0.0.0.0:8815)
    #[arg(long, env = "AS_PROXY_LISTEN_ADDR")]
    listen_addr: Option<String>,

    /// Override Aerospike seed hosts (e.g. 127.0.0.1:3000)
    #[arg(long, env = "AEROSPIKE_HOSTS")]
    aerospike_hosts: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = CliArgs::parse();
    let mut cfg = match args.config {
        Some(ref path) => AppConfig::from_file(path)?,
        None => AppConfig::default(),
    };

    if let Some(addr) = args.listen_addr {
        cfg.server.listen_addr = addr;
    }
    if let Some(hosts) = args.aerospike_hosts {
        cfg.aerospike.hosts = hosts;
    }

    info!(
        listen_addr = %cfg.server.listen_addr,
        metrics_addr = %cfg.server.metrics_addr,
        aerospike_hosts = %cfg.aerospike.hosts,
        tls_enabled = cfg.tls.enabled,
        auth_enabled = cfg.auth.enabled,
        use_batch_write = cfg.writer.use_batch_write,
        batch_write_size = cfg.writer.batch_write_size,
        max_in_flight_records = cfg.writer.max_in_flight_records,
        "Starting Arrow Flight -> Aerospike 8+ MPP Proxy Server"
    );

    // Initialize Aerospike 8+ Community Edition async client (no auth)
    let mut client_policy = ClientPolicy::default();
    client_policy.auth_mode = AuthMode::None;
    client_policy.timeout = cfg.aerospike.timeout_ms;
    client_policy.connect_timeout = cfg.aerospike.connect_timeout_ms;
    client_policy.min_conns_per_node = cfg.aerospike.min_conns_per_node;
    client_policy.max_conns_per_node = cfg.aerospike.max_conns_per_node;
    client_policy.conn_pools_per_node = cfg.aerospike.conn_pools_per_node;

    let as_client = Arc::new(
        Client::new(&client_policy, &cfg.aerospike.hosts)
            .await
            .map_err(|e| format!("Failed to connect to Aerospike at {}: {e}", cfg.aerospike.hosts))?,
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

    // Spawn HTTP observability server (/metrics and /healthz)
    let metrics_addr = cfg.server.metrics_addr.clone();
    let metrics_clone = Arc::clone(&metrics);
    let engine_for_health = Arc::clone(&write_engine);
    tokio::spawn(async move {
        if let Err(e) = serve_observability(&metrics_addr, metrics_clone, move || {
            engine_for_health.is_connected()
        })
        .await
        {
            tracing::error!("Observability server failed: {e}");
        }
    });

    let authenticator = Arc::new(Authenticator::new(cfg.auth.clone()));
    let flight_service = ProxyFlightService::new(
        authenticator,
        write_engine,
        metrics,
        cfg.aerospike.max_bin_name_len,
    );

    let max_msg_bytes = cfg.server.max_decoding_message_size_mb * 1024 * 1024;
    let svc = FlightServiceServer::new(flight_service)
        .max_decoding_message_size(max_msg_bytes)
        .max_encoding_message_size(max_msg_bytes);

    let addr: SocketAddr = cfg.server.listen_addr.parse()?;
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

    info!("Arrow Flight gRPC server listening on {addr}");
    server_builder.add_service(svc).serve(addr).await?;

    Ok(())
}
