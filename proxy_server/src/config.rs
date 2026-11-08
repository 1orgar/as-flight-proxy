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
    /// TTL (in seconds) for keeping Spark job load statistics in the Raft state machine.
    /// Jobs not updated within `job_stats_ttl_secs` are automatically evicted.
    #[serde(default = "default_job_stats_ttl_secs")]
    pub job_stats_ttl_secs: u64,
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
            job_stats_ttl_secs: default_job_stats_ttl_secs(),
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
    /// Data Quality (DQ) validation mode: read records from Aerospike and compare against incoming Spark batch.
    #[serde(alias = "dq", alias = "verify")]
    Validate,
}

impl WriteMode {
    pub fn to_record_exists_action(self) -> RecordExistsAction {
        match self {
            WriteMode::Update | WriteMode::Validate => RecordExistsAction::Update,
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
            "validate" | "dq" | "verify" => Ok(WriteMode::Validate),
            other => Err(ProxyError::InvalidArgument(format!(
                "Unsupported write_mode '{other}'. Expected 'update', 'replace', 'update_only', 'replace_only', 'create_only', or 'validate' ('dq')"
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
    /// Data Quality (DQ) validation mode: instead of writing to Aerospike, read the records from
    /// Aerospike and compare them with the incoming Spark batch (treating `NaN == NaN` as `true`).
    #[serde(default, alias = "dq")]
    pub validate_only: bool,
}

impl WriteParameters {
    pub fn is_validate_mode(&self) -> bool {
        self.validate_only || self.write_mode == WriteMode::Validate
    }
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
        let validate_only = get_str("x-as-validate-only")
            .or_else(|| get_str("x-as-dq"))
            .map(|s| s.eq_ignore_ascii_case("true") || s == "1")
            .unwrap_or(write_mode == WriteMode::Validate);

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
            validate_only,
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

/// Behavior when an Aerospike bin's runtime value type does not match the target Arrow schema column type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TypeMismatchMode {
    /// Fail the read/lookup request immediately with a conversion error.
    #[default]
    Fail,
    /// Substitute `NULL` for the mismatched cell and continue reading.
    Null,
}

impl TypeMismatchMode {
    pub fn from_str_insensitive(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "fail" | "error" | "strict" => Ok(TypeMismatchMode::Fail),
            "null" | "ignore" | "permissive" => Ok(TypeMismatchMode::Null),
            other => Err(ProxyError::InvalidArgument(format!(
                "Unsupported type_mismatch_mode '{other}'. Expected 'fail' or 'null'"
            ))),
        }
    }
}

/// Join semantics when enriching a DataFrame of primary keys via `DoExchange`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum LookupJoinType {
    /// Keep all input rows; if a key is missing in Aerospike, populate all requested bins with `NULL`.
    #[default]
    Left,
    /// Emit only rows whose primary key exists in Aerospike.
    Inner,
}

impl LookupJoinType {
    pub fn from_str_insensitive(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "left" | "left_outer" => Ok(LookupJoinType::Left),
            "inner" => Ok(LookupJoinType::Inner),
            other => Err(ProxyError::InvalidArgument(format!(
                "Unsupported lookup join_type '{other}'. Expected 'left' or 'inner'"
            ))),
        }
    }
}

/// Typed primary key literal for `BatchKeys` reads.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum KeyLiteral {
    String(String),
    Int(i64),
    Bytes(Vec<u8>),
}

impl KeyLiteral {
    pub fn to_aerospike_value(&self) -> aerospike::Value {
        match self {
            KeyLiteral::String(s) => aerospike::Value::String(s.clone()),
            KeyLiteral::Int(i) => aerospike::Value::Int(*i),
            KeyLiteral::Bytes(b) => aerospike::Value::Blob(b.clone()),
        }
    }

    pub fn to_key(&self, namespace: &str, set_name: &str) -> Result<aerospike::Key> {
        aerospike::Key::new(namespace, set_name, self.to_aerospike_value()).map_err(|e| {
            ProxyError::InvalidArgument(format!(
                "Failed to build Aerospike Key for literal {self:?}: {e}"
            ))
        })
    }
}

/// Read execution strategy for `DoGet`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum ReadModeSpec {
    /// Parallel scan over a contiguous range of Aerospike's 4096 partitions `[begin_partition, begin_partition + partition_count)`.
    PartitionRange {
        begin_partition: usize,
        partition_count: usize,
    },
    /// Direct `batch_get` lookup for a specific list of primary keys.
    BatchKeys {
        keys: Vec<KeyLiteral>,
    },
}

/// JSON payload encoded inside `Ticket.ticket` for `DoGet` read streams.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReadTicket {
    pub namespace: String,
    #[serde(default)]
    pub set_name: String,
    pub key_column: String,
    #[serde(default)]
    pub bins: Vec<String>,
    pub read_mode: ReadModeSpec,
    #[serde(default = "default_read_batch_size")]
    pub batch_size: usize,
    #[serde(default)]
    pub type_mismatch_mode: TypeMismatchMode,
    /// Base64-encoded Arrow IPC Schema bytes representing the exact output schema expected by Spark.
    pub schema_ipc_base64: String,
}

impl ReadTicket {
    pub fn validate(&self) -> Result<()> {
        if self.namespace.trim().is_empty() {
            return Err(ProxyError::InvalidArgument(
                "ReadTicket.namespace cannot be empty".to_string(),
            ));
        }
        if self.key_column.trim().is_empty() {
            return Err(ProxyError::InvalidArgument(
                "ReadTicket.key_column cannot be empty".to_string(),
            ));
        }
        if let ReadModeSpec::PartitionRange {
            begin_partition,
            partition_count,
        } = self.read_mode
        {
            if begin_partition >= 4096 || partition_count == 0 || begin_partition + partition_count > 4096 {
                return Err(ProxyError::InvalidArgument(format!(
                    "Invalid Aerospike partition range: begin={begin_partition}, count={partition_count} (must be within 0..4096)"
                )));
            }
        }
        Ok(())
    }
}

/// JSON payload encoded inside `FlightDescriptor.cmd` for `get_schema` (schema inference by sampling).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InferSchemaRequest {
    pub namespace: String,
    #[serde(default)]
    pub set_name: String,
    pub key_column: String,
    #[serde(default = "default_sample_size")]
    pub sample_size: usize,
}

impl InferSchemaRequest {
    pub fn validate(&self) -> Result<()> {
        if self.namespace.trim().is_empty() {
            return Err(ProxyError::InvalidArgument(
                "InferSchemaRequest.namespace cannot be empty".to_string(),
            ));
        }
        if self.key_column.trim().is_empty() {
            return Err(ProxyError::InvalidArgument(
                "InferSchemaRequest.key_column cannot be empty".to_string(),
            ));
        }
        Ok(())
    }
}

/// JSON payload encoded inside `FlightDescriptor.cmd` for `do_exchange` (distributed DataFrame key lookup).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LookupExchangeParams {
    pub namespace: String,
    #[serde(default)]
    pub set_name: String,
    pub key_column: String,
    #[serde(default)]
    pub join_type: LookupJoinType,
    #[serde(default)]
    pub type_mismatch_mode: TypeMismatchMode,
    /// Base64-encoded Arrow IPC Schema bytes for the bins to fetch from Aerospike.
    pub bins_schema_ipc_base64: String,
}

impl LookupExchangeParams {
    pub fn validate(&self) -> Result<()> {
        if self.namespace.trim().is_empty() {
            return Err(ProxyError::InvalidArgument(
                "LookupExchangeParams.namespace cannot be empty".to_string(),
            ));
        }
        if self.key_column.trim().is_empty() {
            return Err(ProxyError::InvalidArgument(
                "LookupExchangeParams.key_column cannot be empty".to_string(),
            ));
        }
        Ok(())
    }
}

fn default_read_batch_size() -> usize {
    4096
}
fn default_sample_size() -> usize {
    1000
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
fn default_job_stats_ttl_secs() -> u64 {
    3600
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
