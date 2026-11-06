use aerospike::{Expiration, RecordExistsAction};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use tonic::metadata::MetadataMap;

use crate::error::{ProxyError, Result};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AppConfig {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub raft: RaftConfig,
    #[serde(default)]
    pub tls: TlsConfig,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub aerospike: AerospikeConfig,
    #[serde(default)]
    pub writer: WriterConfig,
    #[serde(default)]
    pub retry: RetryConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            raft: RaftConfig::default(),
            tls: TlsConfig::default(),
            auth: AuthConfig::default(),
            aerospike: AerospikeConfig::default(),
            writer: WriterConfig::default(),
            retry: RetryConfig::default(),
        }
    }
}

impl AppConfig {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let content = fs::read_to_string(path.as_ref()).map_err(|e| {
            ProxyError::Config(format!(
                "Failed to read config file {}: {e}",
                path.as_ref().display()
            ))
        })?;
        toml::from_str(&content)
            .map_err(|e| ProxyError::Config(format!("Failed to parse TOML config: {e}")))
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerConfig {
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,
    #[serde(default = "default_metrics_addr")]
    pub metrics_addr: String,
    #[serde(default = "default_max_msg_mb")]
    pub max_decoding_message_size_mb: usize,
    #[serde(default = "default_true")]
    pub tcp_nodelay: bool,
    #[serde(default = "default_stream_win_mb")]
    pub http2_stream_window_size_mb: u32,
    #[serde(default = "default_conn_win_mb")]
    pub http2_connection_window_size_mb: u32,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: default_listen_addr(),
            metrics_addr: default_metrics_addr(),
            max_decoding_message_size_mb: default_max_msg_mb(),
            tcp_nodelay: true,
            http2_stream_window_size_mb: default_stream_win_mb(),
            http2_connection_window_size_mb: default_conn_win_mb(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct RaftPeer {
    pub node_id: u64,
    pub raft_addr: String,
    #[serde(default)]
    pub flight_addr: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RaftConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_node_id")]
    pub node_id: u64,
    #[serde(default = "default_raft_addr")]
    pub listen_addr: String,
    #[serde(default = "default_election_min_ms")]
    pub election_timeout_min_ms: u64,
    #[serde(default = "default_election_max_ms")]
    pub election_timeout_max_ms: u64,
    #[serde(default = "default_heartbeat_ms")]
    pub heartbeat_interval_ms: u64,
    #[serde(default)]
    pub peers: Vec<RaftPeer>,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            node_id: default_node_id(),
            listen_addr: default_raft_addr(),
            election_timeout_min_ms: default_election_min_ms(),
            election_timeout_max_ms: default_election_max_ms(),
            heartbeat_interval_ms: default_heartbeat_ms(),
            peers: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct TlsConfig {
    #[serde(default)]
    pub enabled: bool,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
    pub client_ca_path: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AuthConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_username")]
    pub username: String,
    #[serde(default = "default_password")]
    pub password: String,
    #[serde(default = "default_token_secret")]
    pub token_secret: String,
    #[serde(default = "default_token_ttl")]
    pub token_ttl_secs: u64,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            username: default_username(),
            password: default_password(),
            token_secret: default_token_secret(),
            token_ttl_secs: default_token_ttl(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AerospikeConfig {
    #[serde(default = "default_as_hosts")]
    pub hosts: String,
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u32,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u32,
    #[serde(default = "default_min_conns")]
    pub min_conns_per_node: usize,
    #[serde(default = "default_max_conns")]
    pub max_conns_per_node: usize,
    #[serde(default = "default_conn_pools")]
    pub conn_pools_per_node: u8,
    #[serde(default = "default_max_bin_name_len")]
    pub max_bin_name_len: usize,
}

impl Default for AerospikeConfig {
    fn default() -> Self {
        Self {
            hosts: default_as_hosts(),
            connect_timeout_ms: default_connect_timeout_ms(),
            timeout_ms: default_timeout_ms(),
            min_conns_per_node: default_min_conns(),
            max_conns_per_node: default_max_conns(),
            conn_pools_per_node: default_conn_pools(),
            max_bin_name_len: default_max_bin_name_len(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WriterConfig {
    #[serde(default = "default_true")]
    pub use_batch_write: bool,
    #[serde(default = "default_batch_write_size")]
    pub batch_write_size: usize,
    #[serde(default = "default_max_in_flight")]
    pub max_in_flight_records: usize,
    #[serde(default = "default_max_subbatches")]
    pub max_concurrent_subbatches_per_stream: usize,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            use_batch_write: true,
            batch_write_size: default_batch_write_size(),
            max_in_flight_records: default_max_in_flight(),
            max_concurrent_subbatches_per_stream: default_max_subbatches(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RetryConfig {
    #[serde(default = "default_max_other_retries")]
    pub max_other_retries: u32,
    #[serde(default = "default_initial_backoff_ms")]
    pub initial_backoff_ms: u64,
    #[serde(default = "default_max_backoff_ms")]
    pub max_backoff_ms: u64,
    #[serde(default = "default_backoff_multiplier")]
    pub backoff_multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_other_retries: default_max_other_retries(),
            initial_backoff_ms: default_initial_backoff_ms(),
            max_backoff_ms: default_max_backoff_ms(),
            backoff_multiplier: default_backoff_multiplier(),
        }
    }
}

/// Write mode specified per Spark DataFrame write operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    /// Update only the specified bins (merge with existing record bins).
    #[default]
    Update,
    /// Replace the entire record (remove any existing bins not present in the DataFrame).
    Replace,
    /// Update existing record only; fail if record does not exist.
    UpdateOnly,
    /// Replace existing record only; fail if record does not exist.
    ReplaceOnly,
    /// Create new record only; fail if record already exists.
    CreateOnly,
}

impl WriteMode {
    pub fn to_record_exists_action(self) -> RecordExistsAction {
        match self {
            WriteMode::Update => RecordExistsAction::Update,
            WriteMode::Replace => RecordExistsAction::Replace,
            WriteMode::UpdateOnly => RecordExistsAction::UpdateOnly,
            WriteMode::ReplaceOnly => RecordExistsAction::ReplaceOnly,
            WriteMode::CreateOnly => RecordExistsAction::CreateOnly,
        }
    }

    pub fn from_str_insensitive(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "update" | "bins" => Ok(WriteMode::Update),
            "replace" | "row" | "full" => Ok(WriteMode::Replace),
            "update_only" | "updateonly" => Ok(WriteMode::UpdateOnly),
            "replace_only" | "replaceonly" => Ok(WriteMode::ReplaceOnly),
            "create_only" | "createonly" => Ok(WriteMode::CreateOnly),
            other => Err(ProxyError::InvalidArgument(format!(
                "Unsupported write_mode '{other}'. Expected 'update', 'replace', 'update_only', 'replace_only', or 'create_only'"
            ))),
        }
    }
}

/// Per-stream write parameters sent by `spark_connector` inside `FlightDescriptor.cmd` (JSON)
/// or gRPC request metadata headers.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WriteParameters {
    #[serde(default)]
    pub job_id: Option<String>,
    pub namespace: String,
    #[serde(default)]
    pub set_name: String,
    pub key_column: String,
    #[serde(default)]
    pub write_mode: WriteMode,
    #[serde(default)]
    pub ttl_seconds: Option<i64>,
    #[serde(default = "default_true")]
    pub send_key: bool,
    /// By default `false`: in `update` mode, `null` column values send `Value::Nil` to delete the bin.
    /// If `true`, `null` column values are skipped.
    #[serde(default)]
    pub ignore_nulls: bool,
    #[serde(default)]
    pub use_batch_write: Option<bool>,
}

impl WriteParameters {
    pub fn from_descriptor_or_headers(
        descriptor_cmd: Option<&[u8]>,
        headers: &MetadataMap,
    ) -> Result<Self> {
        if let Some(cmd) = descriptor_cmd {
            if !cmd.is_empty() {
                let params: WriteParameters = serde_json::from_slice(cmd).map_err(|e| {
                    ProxyError::InvalidArgument(format!(
                        "Failed to parse WriteParameters JSON from FlightDescriptor.cmd: {e}"
                    ))
                })?;
                params.validate()?;
                return Ok(params);
            }
        }

        // Fallback to gRPC metadata headers
        let get_str = |key: &str| -> Option<String> {
            headers
                .get(key)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string())
        };

        let job_id = get_str("x-as-job-id");
        let namespace = get_str("x-as-namespace").ok_or_else(|| {
            ProxyError::InvalidArgument(
                "Missing WriteParameters: provide FlightDescriptor.cmd JSON or 'x-as-namespace' header"
                    .to_string(),
            )
        })?;
        let set_name = get_str("x-as-set").unwrap_or_default();
        let key_column = get_str("x-as-key-column").ok_or_else(|| {
            ProxyError::InvalidArgument("Missing 'key_column' / 'x-as-key-column'".to_string())
        })?;
        let write_mode = match get_str("x-as-write-mode") {
            Some(s) => WriteMode::from_str_insensitive(&s)?,
            None => WriteMode::Update,
        };
        let ttl_seconds = match get_str("x-as-ttl") {
            Some(s) => Some(s.parse::<i64>().map_err(|e| {
                ProxyError::InvalidArgument(format!("Invalid x-as-ttl '{s}': {e}"))
            })?),
            None => None,
        };
        let send_key = get_str("x-as-send-key")
            .map(|s| s.eq_ignore_ascii_case("true") || s == "1")
            .unwrap_or(true);
        let ignore_nulls = get_str("x-as-ignore-nulls")
            .map(|s| s.eq_ignore_ascii_case("true") || s == "1")
            .unwrap_or(false);
        let use_batch_write = get_str("x-as-use-batch-write")
            .map(|s| s.eq_ignore_ascii_case("true") || s == "1");

        let params = Self {
            job_id,
            namespace,
            set_name,
            key_column,
            write_mode,
            ttl_seconds,
            send_key,
            ignore_nulls,
            use_batch_write,
        };
        params.validate()?;
        Ok(params)
    }

    pub fn validate(&self) -> Result<()> {
        if self.namespace.trim().is_empty() {
            return Err(ProxyError::InvalidArgument(
                "namespace cannot be empty".to_string(),
            ));
        }
        if self.key_column.trim().is_empty() {
            return Err(ProxyError::InvalidArgument(
                "key_column cannot be empty".to_string(),
            ));
        }
        Ok(())
    }

    pub fn expiration(&self) -> Expiration {
        match self.ttl_seconds {
            None | Some(0) => Expiration::NamespaceDefault,
            Some(-1) => Expiration::Never,
            Some(-2) => Expiration::DontUpdate,
            Some(secs) if secs > 0 => Expiration::Seconds(secs as u32),
            Some(_) => Expiration::NamespaceDefault,
        }
    }
}

fn default_listen_addr() -> String {
    "0.0.0.0:8815".to_string()
}
fn default_metrics_addr() -> String {
    "0.0.0.0:9090".to_string()
}
fn default_node_id() -> u64 {
    1
}
fn default_raft_addr() -> String {
    "0.0.0.0:8816".to_string()
}
fn default_election_min_ms() -> u64 {
    150
}
fn default_election_max_ms() -> u64 {
    300
}
fn default_heartbeat_ms() -> u64 {
    50
}
fn default_max_msg_mb() -> usize {
    64
}
fn default_true() -> bool {
    true
}
fn default_stream_win_mb() -> u32 {
    16
}
fn default_conn_win_mb() -> u32 {
    32
}
fn default_username() -> String {
    "spark_writer".to_string()
}
fn default_password() -> String {
    "spark_secret".to_string()
}
fn default_token_secret() -> String {
    "default_hmac_token_secret_change_me".to_string()
}
fn default_token_ttl() -> u64 {
    86400
}
fn default_as_hosts() -> String {
    "127.0.0.1:3000".to_string()
}
fn default_connect_timeout_ms() -> u32 {
    5000
}
fn default_timeout_ms() -> u32 {
    2000
}
fn default_min_conns() -> usize {
    16
}
fn default_max_conns() -> usize {
    512
}
fn default_conn_pools() -> u8 {
    4
}
fn default_max_bin_name_len() -> usize {
    64
}
fn default_batch_write_size() -> usize {
    512
}
fn default_max_in_flight() -> usize {
    16384
}
fn default_max_subbatches() -> usize {
    16
}
fn default_max_other_retries() -> u32 {
    5
}
fn default_initial_backoff_ms() -> u64 {
    10
}
fn default_max_backoff_ms() -> u64 {
    2000
}
fn default_backoff_multiplier() -> f64 {
    2.0
}
