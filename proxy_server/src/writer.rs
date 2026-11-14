use aerospike::operations;
use aerospike::query::PartitionFilter;
use aerospike::{
    BatchOperation, BatchPolicy, BatchReadPolicy, BatchWritePolicy, Bins, Client, ClientResultCode,
    Concurrency, ErrorKind, Key, QueryPolicy, ResultCode, Statement, Value, WritePolicy,
};
use futures::stream::{FuturesUnordered, StreamExt};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{Semaphore, mpsc};
use tracing::{debug, warn};

use crate::config::{RetryConfig, WriteParameters, WriterConfig};
use crate::converter::{
    PreparedRecord, RecordDqResult, ScannedRecord, convert_record_batch_with_key_index,
    validate_record_against_bins, validate_record_batch_schema,
};
use crate::error::{ProxyError, Result};
use crate::metrics::ProxyMetrics;
use arrow_array::RecordBatch;

/// Outcome of attempting to write a single record to Aerospike.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordWriteOutcome {
    /// Record was written successfully.
    Success,
    /// Overload / resource exhaustion error:
    /// `DEVICE_OVERLOAD`, `KEY_BUSY`, `NO_AVAILABLE_CONNECTIONS_TO_NODE`, `MAX_RETRIES_EXCEEDED`.
    /// Must be retried indefinitely ("до талого") with exponential backoff.
    OverloadError(String),
    /// Any other error. Must be retried at most `max_other_retries` times before terminating the stream.
    OtherError(String),
}

/// Outcome of attempting to read a single record from Aerospike during Data Quality (DQ) validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordReadOutcome {
    /// Record was found in Aerospike with its bins.
    Found(HashMap<String, Value>),
    /// Record does not exist in Aerospike (`ResultCode::KeyNotFoundError`).
    NotFound,
    /// Overload / resource exhaustion error: retried indefinitely ("до талого") with exponential backoff.
    OverloadError(String),
    /// Any other error: retried at most `max_other_retries` times.
    OtherError(String),
}

/// Sample of a Data Quality (DQ) validation mismatch for a specific key.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DqMismatchSample {
    pub key: String,
    pub reason: String,
}

/// Statistics returned after writing or validating an Arrow `RecordBatch`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BatchWriteStats {
    pub rows_written: usize,
    pub overload_retries: u64,
    pub other_retries: u64,
    pub rows_validated: usize,
    pub matched_rows: usize,
    pub mismatched_rows: usize,
    pub missing_keys: usize,
    pub bin_mismatch_rows: usize,
    pub mismatch_samples: Vec<DqMismatchSample>,
}

/// Abstraction over the Aerospike client for executing batch/single-key writes and DQ batch reads.
#[tonic::async_trait]
pub trait AerospikeSink: Send + Sync + 'static {
    async fn execute_batch_write(
        &self,
        records: &[PreparedRecord],
        params: &WriteParameters,
    ) -> Vec<RecordWriteOutcome>;

    async fn execute_single_put(
        &self,
        record: &PreparedRecord,
        params: &WriteParameters,
    ) -> RecordWriteOutcome;

    async fn execute_batch_read(
        &self,
        records: &[PreparedRecord],
        params: &WriteParameters,
    ) -> Vec<RecordReadOutcome>;

    async fn execute_batch_read_keys(
        &self,
        keys: &[Key],
        bins: Option<&[String]>,
    ) -> Vec<RecordReadOutcome> {
        let dummy_params = WriteParameters {
            job_id: None,
            namespace: keys
                .first()
                .map(|k| k.namespace().to_string())
                .unwrap_or_else(|| "test".to_string()),
            set_name: keys
                .first()
                .map(|k| k.set_name().to_string())
                .unwrap_or_default(),
            key_column: "pk".to_string(),
            write_mode: crate::config::WriteMode::Update,
            ttl_seconds: None,
            send_key: true,
            ignore_nulls: false,
            use_batch_write: Some(true),
            validate_only: true,
            compression: None,
            auto_convert_strings: false,
        };
        let recs: Vec<PreparedRecord> = keys
            .iter()
            .map(|k| PreparedRecord {
                key: k.clone(),
                bins: Vec::new(),
            })
            .collect();
        let mut outcomes = self.execute_batch_read(&recs, &dummy_params).await;
        if let Some(requested_bins) = bins {
            for outcome in &mut outcomes {
                if let RecordReadOutcome::Found(map) = outcome {
                    map.retain(|k, _| requested_bins.iter().any(|b| b == k));
                }
            }
        }
        outcomes
    }

    async fn scan_partition_range(
        &self,
        namespace: &str,
        set_name: &str,
        begin_partition: usize,
        partition_count: usize,
        bins: Option<Vec<String>>,
        max_records: Option<u64>,
    ) -> Result<mpsc::Receiver<Result<ScannedRecord>>> {
        let _ = (
            namespace,
            set_name,
            begin_partition,
            partition_count,
            bins,
            max_records,
        );
        Err(ProxyError::Internal(
            "scan_partition_range is not implemented for this sink".to_string(),
        ))
    }

    fn is_connected(&self) -> bool;
}

/// Production implementation backed by `aerospike::Client` (v3.0 async).
pub struct RealAerospikeSink {
    client: Arc<Client>,
    timeout_ms: u32,
}

impl RealAerospikeSink {
    pub fn new(client: Arc<Client>, timeout_ms: u32) -> Self {
        Self { client, timeout_ms }
    }
}

#[tonic::async_trait]
impl AerospikeSink for RealAerospikeSink {
    async fn execute_batch_write(
        &self,
        records: &[PreparedRecord],
        params: &WriteParameters,
    ) -> Vec<RecordWriteOutcome> {
        if records.is_empty() {
            return Vec::new();
        }

        let mut batch_policy = BatchPolicy::write_default();
        batch_policy.concurrency = Concurrency::Parallel;
        batch_policy.respond_all_keys = true;
        batch_policy.base_policy.socket_timeout = self.timeout_ms;
        batch_policy.base_policy.total_timeout = self.timeout_ms;
        batch_policy.base_policy.max_retries = 0;

        let mut write_policy = BatchWritePolicy::default();
        write_policy.record_exists_action = params.write_mode.to_record_exists_action();
        write_policy.expiration = params.expiration();
        write_policy.send_key = params.send_key;

        let mut batch_ops: Vec<BatchOperation> = Vec::with_capacity(records.len());
        for rec in records {
            let ops: Vec<operations::Operation> = rec.bins.iter().map(operations::put).collect();
            batch_ops.push(BatchOperation::write(&write_policy, rec.key.clone(), ops));
        }

        match self.client.batch(&batch_policy, &mut batch_ops).await {
            Ok(()) => {
                let mut outcomes = Vec::with_capacity(batch_ops.len());
                for (idx, op) in batch_ops.iter().enumerate() {
                    let all_nil = !records[idx].bins.is_empty()
                        && records[idx].bins.iter().all(|b| b.value == Value::Nil);
                    match op.result_code() {
                        Some(ResultCode::Ok) => outcomes.push(RecordWriteOutcome::Success),
                        Some(ResultCode::KeyNotFoundError) if all_nil => {
                            outcomes.push(RecordWriteOutcome::Success);
                        }
                        rc => {
                            let outcome = classify_aerospike_failure(op.error(), rc);
                            outcomes.push(outcome);
                        }
                    }
                }
                outcomes
            }
            Err(call_err) => {
                // Even if `client.batch` returned a top-level error, some rows may have already been
                // answered with `ResultCode::Ok`. Check per-row state first!
                let fallback_outcome = classify_aerospike_failure(Some(&call_err), None);
                let mut outcomes = Vec::with_capacity(batch_ops.len());
                for (idx, op) in batch_ops.iter().enumerate() {
                    let all_nil = !records[idx].bins.is_empty()
                        && records[idx].bins.iter().all(|b| b.value == Value::Nil);
                    if op.result_code() == Some(ResultCode::Ok)
                        || (all_nil && op.result_code() == Some(ResultCode::KeyNotFoundError))
                    {
                        outcomes.push(RecordWriteOutcome::Success);
                    } else if op.error().is_some() || op.result_code().is_some() {
                        outcomes.push(classify_aerospike_failure(op.error(), op.result_code()));
                    } else {
                        outcomes.push(fallback_outcome.clone());
                    }
                }
                outcomes
            }
        }
    }

    async fn execute_single_put(
        &self,
        record: &PreparedRecord,
        params: &WriteParameters,
    ) -> RecordWriteOutcome {
        let mut wp = WritePolicy::default();
        wp.base_policy.socket_timeout = self.timeout_ms;
        wp.base_policy.total_timeout = self.timeout_ms;
        wp.base_policy.max_retries = 0;
        wp.record_exists_action = params.write_mode.to_record_exists_action();
        wp.expiration = params.expiration();
        wp.send_key = params.send_key;

        let all_nil = !record.bins.is_empty() && record.bins.iter().all(|b| b.value == Value::Nil);

        match self.client.put(&wp, &record.key, &record.bins).await {
            Ok(()) => RecordWriteOutcome::Success,
            Err(err) => {
                if all_nil && err.matches(&[ResultCode::KeyNotFoundError]) {
                    RecordWriteOutcome::Success
                } else {
                    classify_aerospike_failure(Some(&err), None)
                }
            }
        }
    }

    async fn execute_batch_read(
        &self,
        records: &[PreparedRecord],
        _params: &WriteParameters,
    ) -> Vec<RecordReadOutcome> {
        if records.is_empty() {
            return Vec::new();
        }

        let mut batch_policy = BatchPolicy::default();
        batch_policy.concurrency = Concurrency::Parallel;
        batch_policy.respond_all_keys = true;
        batch_policy.base_policy.socket_timeout = self.timeout_ms;
        batch_policy.base_policy.total_timeout = self.timeout_ms;
        batch_policy.base_policy.max_retries = 0;

        let read_policy = BatchReadPolicy::default();
        let mut batch_ops: Vec<BatchOperation> = records
            .iter()
            .map(|rec| {
                let bins = if rec.bins.is_empty() {
                    Bins::None
                } else {
                    Bins::Some(rec.bins.iter().map(|b| b.name.clone()).collect())
                };
                BatchOperation::read(&read_policy, rec.key.clone(), bins)
            })
            .collect();

        match self.client.batch(&batch_policy, &mut batch_ops).await {
            Ok(()) => batch_ops.iter().map(classify_batch_read_op).collect(),
            Err(call_err) => {
                let fallback_write = classify_aerospike_failure(Some(&call_err), None);
                let fallback_read = match fallback_write {
                    RecordWriteOutcome::OverloadError(m) => RecordReadOutcome::OverloadError(m),
                    RecordWriteOutcome::OtherError(_) | RecordWriteOutcome::Success => {
                        RecordReadOutcome::OtherError(call_err.to_string())
                    }
                };
                batch_ops
                    .iter()
                    .map(|op| {
                        if op.result_code().is_some() || op.error().is_some() {
                            classify_batch_read_op(op)
                        } else {
                            fallback_read.clone()
                        }
                    })
                    .collect()
            }
        }
    }

    async fn execute_batch_read_keys(
        &self,
        keys: &[Key],
        bins: Option<&[String]>,
    ) -> Vec<RecordReadOutcome> {
        if keys.is_empty() {
            return Vec::new();
        }

        let mut batch_policy = BatchPolicy::default();
        batch_policy.concurrency = Concurrency::Parallel;
        batch_policy.respond_all_keys = true;
        batch_policy.base_policy.socket_timeout = self.timeout_ms;
        batch_policy.base_policy.total_timeout = self.timeout_ms;
        batch_policy.base_policy.max_retries = 0;

        let read_policy = BatchReadPolicy::default();
        let as_bins = match bins {
            None => Bins::All,
            Some(list) if list.is_empty() => Bins::None,
            Some(list) => Bins::Some(list.to_vec()),
        };

        let mut batch_ops: Vec<BatchOperation> = keys
            .iter()
            .map(|key| BatchOperation::read(&read_policy, key.clone(), as_bins.clone()))
            .collect();

        match self.client.batch(&batch_policy, &mut batch_ops).await {
            Ok(()) => batch_ops.iter().map(classify_batch_read_op).collect(),
            Err(call_err) => {
                let fallback_write = classify_aerospike_failure(Some(&call_err), None);
                let fallback_read = match fallback_write {
                    RecordWriteOutcome::OverloadError(m) => RecordReadOutcome::OverloadError(m),
                    RecordWriteOutcome::OtherError(_) | RecordWriteOutcome::Success => {
                        RecordReadOutcome::OtherError(call_err.to_string())
                    }
                };
                batch_ops
                    .iter()
                    .map(|op| {
                        if op.result_code().is_some() || op.error().is_some() {
                            classify_batch_read_op(op)
                        } else {
                            fallback_read.clone()
                        }
                    })
                    .collect()
            }
        }
    }

    async fn scan_partition_range(
        &self,
        namespace: &str,
        set_name: &str,
        begin_partition: usize,
        partition_count: usize,
        bins: Option<Vec<String>>,
        max_records: Option<u64>,
    ) -> Result<mpsc::Receiver<Result<ScannedRecord>>> {
        let pf = PartitionFilter::by_range(begin_partition, partition_count);
        let mut qpolicy = QueryPolicy::default();
        qpolicy.base_policy.socket_timeout = self.timeout_ms;
        qpolicy.record_queue_size = 4096;
        if let Some(max_rec) = max_records {
            qpolicy.max_records = max_rec;
        }

        let as_bins = match bins {
            None => Bins::All,
            Some(ref list) if list.is_empty() => Bins::None,
            Some(list) => Bins::Some(list),
        };
        let stmt = Statement::new(namespace, set_name, as_bins);
        let rs = self
            .client
            .query(&qpolicy, pf, stmt)
            .await
            .map_err(ProxyError::from)?;

        let (tx, rx) = mpsc::channel::<Result<ScannedRecord>>(4096);
        tokio::spawn(async move {
            let stream = rs.into_stream();
            tokio::pin!(stream);
            while let Some(res) = stream.next().await {
                let item = match res {
                    Ok(rec) => Ok(ScannedRecord {
                        key: rec.key,
                        bins: rec.bins,
                    }),
                    Err(e) => Err(ProxyError::from(e)),
                };
                if tx.send(item).await.is_err() {
                    break;
                }
            }
        });

        Ok(rx)
    }

    fn is_connected(&self) -> bool {
        self.client.is_connected()
    }
}

fn classify_batch_read_op(op: &BatchOperation) -> RecordReadOutcome {
    match op.result_code() {
        Some(ResultCode::Ok) => {
            let bins: HashMap<String, Value> = op
                .record()
                .map(|r| r.bins.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                .unwrap_or_default();
            RecordReadOutcome::Found(bins)
        }
        Some(ResultCode::KeyNotFoundError) => RecordReadOutcome::NotFound,
        rc => match classify_aerospike_failure(op.error(), rc) {
            RecordWriteOutcome::OverloadError(m) => RecordReadOutcome::OverloadError(m),
            RecordWriteOutcome::OtherError(m) => RecordReadOutcome::OtherError(m),
            RecordWriteOutcome::Success => {
                RecordReadOutcome::OtherError("Unexpected non-Ok batch read status".to_string())
            }
        },
    }
}

/// Classify an Aerospike error / result code into either:
/// - `OverloadError` (infinite retry with backoff):
///   `MAX_RETRIES_EXCEEDED`, `NO_AVAILABLE_CONNECTIONS_TO_NODE`, `DEVICE_OVERLOAD`, `KEY_BUSY`
/// - `OtherError` (terminate after N retries)
pub fn classify_aerospike_failure(
    err: Option<&aerospike::Error>,
    rc: Option<ResultCode>,
) -> RecordWriteOutcome {
    // 1. Check explicit server ResultCode if present
    if let Some(code) = rc {
        if matches!(
            code,
            ResultCode::DeviceOverload | ResultCode::KeyBusy | ResultCode::XdrKeyBusy
        ) {
            return RecordWriteOutcome::OverloadError(format!("Server result code: {code:?}"));
        }
    }

    // 2. Check full `aerospike::Error` chain (including sub_errors and causal chain)
    if let Some(e) = err {
        if is_overload_error_recursive(e) {
            return RecordWriteOutcome::OverloadError(e.to_string());
        }
        return RecordWriteOutcome::OtherError(e.to_string());
    }

    if let Some(code) = rc {
        return RecordWriteOutcome::OtherError(format!("Server result code: {code:?}"));
    }

    RecordWriteOutcome::OtherError("Unknown Aerospike write failure (no response)".to_string())
}

fn is_overload_error_recursive(err: &aerospike::Error) -> bool {
    // Server overload / hot-key result codes anywhere in the cause chain
    if err.matches(&[
        ResultCode::DeviceOverload,
        ResultCode::KeyBusy,
        ResultCode::XdrKeyBusy,
    ]) {
        return true;
    }

    // Client overload / connection exhaustion / max-retries-exceeded codes anywhere in the cause chain
    if err.matches_client(&[
        ClientResultCode::MaxRetriesExceeded,
        ClientResultCode::NoMoreConnections,
        ClientResultCode::AsyncQueueFull,
    ]) {
        return true;
    }

    // Check ErrorKind directly
    if matches!(
        err.kind(),
        ErrorKind::NoMoreConnections | ErrorKind::ConnectionPoolEmpty
    ) {
        return true;
    }

    // Also inspect sub_errors from prior retry attempts if any
    for sub in err.sub_errors() {
        if is_overload_error_recursive(sub) {
            return true;
        }
    }

    false
}

/// Snapshot of dynamically computed batching & concurrency parameters for a write slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptiveWritePlan {
    pub chunk_size: usize,
    pub max_concurrency: usize,
    pub pacing_delay_micros: u64,
    pub row_permit_weight: usize,
}

/// Diagnostic snapshot of the on-the-fly adaptive write batching controller.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveTunerSnapshot {
    pub enabled: bool,
    pub batch_scale_permille: u64,
    pub concurrency_scale_permille: u64,
    pub pacing_delay_micros: u64,
    pub queue_inflation_permille: u64,
    pub last_chunk_size: usize,
    pub last_concurrency: usize,
}

struct TunerEpochState {
    min_unit_latency_us: f64,
    ema_unit_latency_us: f64,
    ema_rpc_latency_us: f64,
    prev_goodput_rec_per_sec: f64,
    probe_dir: i8,
    last_overload_backoff_at: Option<Instant>,
    last_wait_backoff_at: Option<Instant>,
    healthy_streak: u32,
}

/// Lock-free hot-path heuristic (BBR queue-inflation avoidance + throughput-gradient hill-climbing)
/// that dynamically tunes sub-batch size, stream concurrency, retry slicing, and micro-pacing
/// to maximize Aerospike write RPS while preventing cluster `wait` / `DEVICE_OVERLOAD` stalls.
pub struct AdaptiveBatchTuner {
    enabled: bool,
    base_batch_size: usize,
    min_batch_size: usize,
    max_batch_size: usize,
    base_stream_concurrency: usize,
    target_latency_us: u64,
    target_batch_bytes: usize,

    batch_scale_permille: AtomicU64,
    concurrency_scale_permille: AtomicU64,
    pacing_delay_micros: AtomicU64,
    last_queue_inflation_permille: AtomicU64,
    last_chunk_size: AtomicUsize,
    last_concurrency: AtomicUsize,

    epoch_rpcs: AtomicU64,
    epoch_records: AtomicU64,
    epoch_work_units_x100: AtomicU64,
    epoch_latency_us: AtomicU64,
    epoch_overload_records: AtomicU64,
    epoch_sem_wait_us: AtomicU64,

    state: Mutex<TunerEpochState>,
}

impl AdaptiveBatchTuner {
    const EPOCH_RPCS: u64 = 8;
    const TARGET_BATCH_BIN_OPS: usize = 24_000;

    pub fn new(config: &WriterConfig) -> Self {
        let base_batch = config.batch_write_size.max(1);
        let min_batch = config.min_batch_write_size.max(1).min(base_batch);
        let max_batch = config.max_batch_write_size.max(base_batch).max(min_batch);
        let base_conc = config.max_concurrent_subbatches_per_stream.max(1);
        let target_latency_us = config.target_write_latency_ms.max(1).saturating_mul(1_000);
        let target_batch_bytes = config.target_batch_bytes.max(4_096);

        Self {
            enabled: config.adaptive_batching,
            base_batch_size: base_batch,
            min_batch_size: min_batch,
            max_batch_size: max_batch,
            base_stream_concurrency: base_conc,
            target_latency_us,
            target_batch_bytes,
            batch_scale_permille: AtomicU64::new(1_000),
            concurrency_scale_permille: AtomicU64::new(1_000),
            pacing_delay_micros: AtomicU64::new(0),
            last_queue_inflation_permille: AtomicU64::new(1_000),
            last_chunk_size: AtomicUsize::new(base_batch),
            last_concurrency: AtomicUsize::new(base_conc),
            epoch_rpcs: AtomicU64::new(0),
            epoch_records: AtomicU64::new(0),
            epoch_work_units_x100: AtomicU64::new(0),
            epoch_latency_us: AtomicU64::new(0),
            epoch_overload_records: AtomicU64::new(0),
            epoch_sem_wait_us: AtomicU64::new(0),
            state: Mutex::new(TunerEpochState {
                min_unit_latency_us: 0.0,
                ema_unit_latency_us: 0.0,
                ema_rpc_latency_us: 0.0,
                prev_goodput_rec_per_sec: 0.0,
                probe_dir: 1,
                last_overload_backoff_at: None,
                last_wait_backoff_at: None,
                healthy_streak: 0,
            }),
        }
    }

    pub fn snapshot(&self) -> AdaptiveTunerSnapshot {
        AdaptiveTunerSnapshot {
            enabled: self.enabled,
            batch_scale_permille: self.batch_scale_permille.load(Ordering::Relaxed),
            concurrency_scale_permille: self.concurrency_scale_permille.load(Ordering::Relaxed),
            pacing_delay_micros: self.pacing_delay_micros.load(Ordering::Relaxed),
            queue_inflation_permille: self.last_queue_inflation_permille.load(Ordering::Relaxed),
            last_chunk_size: self.last_chunk_size.load(Ordering::Relaxed),
            last_concurrency: self.last_concurrency.load(Ordering::Relaxed),
        }
    }

    /// Compute the optimal sub-batch size, stream concurrency, permit weight, and micro-pacing
    /// for the next slice using lock-free atomic reads.
    pub fn plan(
        &self,
        remaining_rows: usize,
        num_bins: usize,
        approx_row_bytes: usize,
        active_streams: usize,
        metrics: &ProxyMetrics,
    ) -> AdaptiveWritePlan {
        let bins = num_bins.max(1);
        if !self.enabled {
            let bin_weight = bins.div_ceil(10).max(1);
            let chunk_size = (self.base_batch_size / bin_weight)
                .max(self.min_batch_size)
                .min(self.base_batch_size);
            metrics.set_adaptive_state(
                chunk_size as u64,
                self.base_stream_concurrency as u64,
                0,
                1_000,
            );
            return AdaptiveWritePlan {
                chunk_size,
                max_concurrency: self.base_stream_concurrency,
                pacing_delay_micros: 0,
                row_permit_weight: bin_weight,
            };
        }

        let effective_row_bytes = approx_row_bytes.max(bins.saturating_mul(12)).max(32);
        let bytes_cap = (self.target_batch_bytes / effective_row_bytes).max(1);
        let ops_cap = (Self::TARGET_BATCH_BIN_OPS / bins).max(1);
        let geom_base = self
            .base_batch_size
            .min(bytes_cap)
            .min(ops_cap)
            .max(self.min_batch_size);

        let b_scale = self.batch_scale_permille.load(Ordering::Relaxed);
        let raw_chunk = ((geom_base as u128 * b_scale as u128) / 1_000) as usize;
        let tuned_capacity = raw_chunk.clamp(self.min_batch_size, self.max_batch_size);

        // Balance remaining rows evenly across chunks so we don't emit tiny tail micro-batches
        let chunk_size = if remaining_rows > tuned_capacity {
            let num_chunks = remaining_rows.div_ceil(tuned_capacity).max(1);
            remaining_rows.div_ceil(num_chunks).max(1)
        } else {
            remaining_rows.max(1).min(tuned_capacity)
        };

        let c_scale = self.concurrency_scale_permille.load(Ordering::Relaxed);
        let streams = active_streams.max(1);
        let max_stream_boost = self.base_stream_concurrency.saturating_mul(2).max(32);
        let scaled_stream_conc = (((self.base_stream_concurrency as u64 * c_scale) / 1_000).max(1)
            as usize)
            .min(max_stream_boost);

        // Fair-share global Aerospike batch-RPC concurrency budget across active DoPut streams
        // so high stream fan-in (e.g. 80..160 Spark tasks) never swamps Aerospike connection pools into wait.
        let global_rpc_budget =
            (((self.base_stream_concurrency.saturating_mul(16) as u64) * c_scale) / 1_000).max(4)
                as usize;
        let fair_share_conc = (global_rpc_budget / streams).max(1);
        let effective_conc = scaled_stream_conc.min(fair_share_conc).max(1);

        let pacing = self.pacing_delay_micros.load(Ordering::Relaxed);
        let q_inf = self.last_queue_inflation_permille.load(Ordering::Relaxed);

        let bin_weight_soft = bins.div_ceil(16).max(1);
        let byte_weight_soft = (effective_row_bytes / 256).max(1);
        let row_permit_weight = bin_weight_soft.max(byte_weight_soft).min(32);

        self.last_chunk_size
            .store(tuned_capacity, Ordering::Relaxed);
        self.last_concurrency
            .store(effective_conc, Ordering::Relaxed);
        metrics.set_adaptive_state(tuned_capacity as u64, effective_conc as u64, pacing, q_inf);

        AdaptiveWritePlan {
            chunk_size,
            max_concurrency: effective_conc,
            pacing_delay_micros: pacing,
            row_permit_weight,
        }
    }

    /// Compute a gentler sub-batch cap when retrying overloaded keys so hot Aerospike partitions
    /// recover without re-triggering `DEVICE_OVERLOAD` or `write-block` contention.
    pub fn retry_chunk_size(&self, pending_len: usize, backoff_step: u32) -> usize {
        if !self.enabled || pending_len <= 1 {
            return pending_len.max(1);
        }
        let cur_cap = self
            .last_chunk_size
            .load(Ordering::Relaxed)
            .max(self.min_batch_size);
        let shift = backoff_step.clamp(1, 3);
        let floor = self.min_batch_size.min(16).max(1);
        (cur_cap >> shift).max(floor).min(pending_len).max(1)
    }

    /// Feed telemetry from a completed Aerospike `batch_write` / `put` slice into the closed-loop controller.
    pub fn observe_rpc(
        &self,
        records: usize,
        num_bins: usize,
        rpc_latency_us: u64,
        overload_records: u64,
        sem_wait_us: u64,
        metrics: &ProxyMetrics,
    ) {
        if !self.enabled || records == 0 {
            return;
        }

        let eff_lat_us = rpc_latency_us.max(10);
        let bin_ops = records.saturating_mul(num_bins.max(1));
        let work_x100 = 1_600u64
            .saturating_add((records as u64).saturating_mul(100))
            .saturating_add((bin_ops as u64).saturating_mul(5));

        let rpcs = self.epoch_rpcs.fetch_add(1, Ordering::Relaxed) + 1;
        self.epoch_records
            .fetch_add(records as u64, Ordering::Relaxed);
        self.epoch_work_units_x100
            .fetch_add(work_x100, Ordering::Relaxed);
        self.epoch_latency_us
            .fetch_add(eff_lat_us, Ordering::Relaxed);
        self.epoch_overload_records
            .fetch_add(overload_records, Ordering::Relaxed);
        self.epoch_sem_wait_us
            .fetch_add(sem_wait_us, Ordering::Relaxed);

        let severe_latency = eff_lat_us > self.target_latency_us.saturating_mul(2);
        if overload_records == 0 && !severe_latency && rpcs < Self::EPOCH_RPCS {
            return;
        }

        let Ok(mut st) = self.state.try_lock() else {
            return;
        };

        let tot_rpcs = self.epoch_rpcs.swap(0, Ordering::Relaxed);
        if tot_rpcs == 0 {
            return;
        }
        let tot_records = self.epoch_records.swap(0, Ordering::Relaxed);
        let tot_work_x100 = self.epoch_work_units_x100.swap(0, Ordering::Relaxed);
        let tot_lat_us = self.epoch_latency_us.swap(0, Ordering::Relaxed).max(1);
        let tot_overloads = self.epoch_overload_records.swap(0, Ordering::Relaxed);
        let tot_sem_wait_us = self.epoch_sem_wait_us.swap(0, Ordering::Relaxed);

        let avg_rpc_lat_us = (tot_lat_us as f64) / (tot_rpcs as f64);
        let avg_sem_wait_us = (tot_sem_wait_us as f64) / (tot_rpcs as f64);
        let unit_lat_us = (tot_lat_us as f64 * 100.0) / (tot_work_x100.max(100) as f64);
        let goodput_rps = (tot_records as f64 * 1_000_000.0) / (tot_lat_us as f64);

        if st.min_unit_latency_us <= 0.0 {
            st.min_unit_latency_us = unit_lat_us;
            st.ema_unit_latency_us = unit_lat_us;
            st.ema_rpc_latency_us = avg_rpc_lat_us;
        } else {
            // Fast decay when queues drain, smooth filtering when latency rises
            if unit_lat_us < st.ema_unit_latency_us {
                st.ema_unit_latency_us = st.ema_unit_latency_us * 0.40 + unit_lat_us * 0.60;
            } else {
                st.ema_unit_latency_us = st.ema_unit_latency_us * 0.65 + unit_lat_us * 0.35;
            }
            if avg_rpc_lat_us < st.ema_rpc_latency_us {
                st.ema_rpc_latency_us = st.ema_rpc_latency_us * 0.40 + avg_rpc_lat_us * 0.60;
            } else {
                st.ema_rpc_latency_us = st.ema_rpc_latency_us * 0.65 + avg_rpc_lat_us * 0.35;
            }
            if unit_lat_us < st.min_unit_latency_us {
                st.min_unit_latency_us = unit_lat_us;
            } else if tot_overloads == 0 {
                // Slow upward baseline adaptation (0.4% per epoch) so topology/workload changes never pin stale min RTT
                st.min_unit_latency_us =
                    (st.min_unit_latency_us * 1.004).min(st.ema_unit_latency_us);
            }
        }

        let min_base = st.min_unit_latency_us.max(1e-6);
        let curr_queue_inflation = (unit_lat_us / min_base).clamp(1.0, 20.0);
        let queue_inflation = (st.ema_unit_latency_us / min_base).clamp(1.0, 20.0);
        let q_inf_permille = (queue_inflation * 1_000.0).round() as u64;
        self.last_queue_inflation_permille
            .store(q_inf_permille, Ordering::Relaxed);

        let now = Instant::now();
        let cur_b = self.batch_scale_permille.load(Ordering::Relaxed);
        let cur_c = self.concurrency_scale_permille.load(Ordering::Relaxed);
        let cur_p = self.pacing_delay_micros.load(Ordering::Relaxed);

        // Regime 1: Explicit Aerospike overload (`DEVICE_OVERLOAD`, `KEY_BUSY`, `NO_MORE_CONNECTIONS`, etc.)
        if tot_overloads > 0 {
            let cooldown_ok = st
                .last_overload_backoff_at
                .is_none_or(|t| now.duration_since(t) >= Duration::from_millis(15));
            if cooldown_ok {
                st.last_overload_backoff_at = Some(now);
                st.last_wait_backoff_at = Some(now);
                st.healthy_streak = 0;
                st.probe_dir = 1;
                st.prev_goodput_rec_per_sec = 0.0;
                let ov_ratio = (tot_overloads as f64 / tot_records.max(1) as f64).clamp(0.05, 1.0);
                let new_c =
                    ((cur_c as f64 * (0.75 - 0.20 * ov_ratio)).round() as u64).clamp(125, 2_500);
                let new_b =
                    ((cur_b as f64 * (0.80 - 0.20 * ov_ratio)).round() as u64).clamp(200, 4_000);
                let new_p = ((400.0 + 3_600.0 * ov_ratio).round() as u64)
                    .max(cur_p)
                    .min(5_000);
                self.batch_scale_permille.store(new_b, Ordering::Relaxed);
                self.concurrency_scale_permille
                    .store(new_c, Ordering::Relaxed);
                self.pacing_delay_micros.store(new_p, Ordering::Relaxed);
                metrics.inc_cluster_wait_events();
                metrics.set_adaptive_state(
                    self.last_chunk_size.load(Ordering::Relaxed) as u64,
                    self.last_concurrency.load(Ordering::Relaxed) as u64,
                    new_p,
                    q_inf_permille,
                );
                debug!(
                    overload_records = tot_overloads,
                    batch_scale_permille = new_b,
                    concurrency_scale_permille = new_c,
                    pacing_delay_micros = new_p,
                    "Adaptive write tuner backed off on Aerospike overload"
                );
            }
            return;
        }

        // Regime 2: Proactive Aerospike cluster queue-wait avoidance (BBR/Vegas latency inflation before errors!)
        let target_us_f = self.target_latency_us.max(1) as f64;
        let epoch_elevated = curr_queue_inflation > 1.35
            || avg_rpc_lat_us > target_us_f * 0.85
            || avg_sem_wait_us > target_us_f * 0.85;
        let cluster_waiting = epoch_elevated
            && (queue_inflation > 1.55
                || st.ema_rpc_latency_us > target_us_f
                || avg_sem_wait_us > target_us_f);
        if cluster_waiting {
            let cooldown_ok = st
                .last_wait_backoff_at
                .is_none_or(|t| now.duration_since(t) >= Duration::from_millis(10));
            if cooldown_ok {
                st.last_wait_backoff_at = Some(now);
                st.healthy_streak = 0;
                st.probe_dir = 1;
                st.prev_goodput_rec_per_sec = 0.0;
                let lat_ratio = st.ema_rpc_latency_us / target_us_f;
                let severity = (queue_inflation - 1.40)
                    .max(lat_ratio - 0.90)
                    .clamp(0.10, 1.0);
                let trim = (0.06 + 0.19 * severity).clamp(0.06, 0.25);
                let new_c = ((cur_c as f64 * (1.0 - trim)).round() as u64).clamp(125, 2_500);
                let new_b = ((cur_b as f64 * (1.0 - trim * 0.75)).round() as u64).clamp(200, 4_000);
                let target_pacing =
                    ((st.ema_rpc_latency_us * trim * 0.25).round() as u64).clamp(100, 3_000);
                let new_p = ((cur_p + target_pacing) / 2).max(target_pacing).min(3_000);
                self.batch_scale_permille.store(new_b, Ordering::Relaxed);
                self.concurrency_scale_permille
                    .store(new_c, Ordering::Relaxed);
                self.pacing_delay_micros.store(new_p, Ordering::Relaxed);
                metrics.inc_cluster_wait_events();
                metrics.set_adaptive_state(
                    self.last_chunk_size.load(Ordering::Relaxed) as u64,
                    self.last_concurrency.load(Ordering::Relaxed) as u64,
                    new_p,
                    q_inf_permille,
                );
                debug!(
                    queue_inflation_permille = q_inf_permille,
                    ema_rpc_latency_us = st.ema_rpc_latency_us as u64,
                    batch_scale_permille = new_b,
                    concurrency_scale_permille = new_c,
                    pacing_delay_micros = new_p,
                    "Adaptive write tuner proactively throttled to avoid Aerospike cluster wait"
                );
            }
            return;
        }

        // Regime 3: Healthy, wait-free Aerospike cluster -> drain pacing and hill-climb for maximum RPS
        let new_p = if cur_p <= 80 { 0 } else { cur_p / 2 };
        self.pacing_delay_micros.store(new_p, Ordering::Relaxed);
        metrics.set_adaptive_state(
            self.last_chunk_size.load(Ordering::Relaxed) as u64,
            self.last_concurrency.load(Ordering::Relaxed) as u64,
            new_p,
            q_inf_permille,
        );

        if (queue_inflation <= 1.35 || curr_queue_inflation <= 1.25)
            && avg_rpc_lat_us <= target_us_f * 0.85
        {
            st.healthy_streak = st.healthy_streak.saturating_add(1);
            let avg_rec_per_rpc = (tot_records as f64) / (tot_rpcs as f64);
            let last_cap = self.last_chunk_size.load(Ordering::Relaxed).max(1) as f64;
            let full_batches = avg_rec_per_rpc >= last_cap * 0.70;

            if st.prev_goodput_rec_per_sec > 0.0 && full_batches {
                let rel_gain =
                    (goodput_rps - st.prev_goodput_rec_per_sec) / st.prev_goodput_rec_per_sec;
                if rel_gain < -0.06 && cur_b > 800 {
                    st.probe_dir = -st.probe_dir;
                } else if st.probe_dir < 0 && rel_gain < -0.02 {
                    st.probe_dir = 1;
                }
            }
            if cur_b <= 300 {
                st.probe_dir = 1;
            } else if cur_b >= 3_800 {
                st.probe_dir = -1;
            }

            let step: i64 = if st.healthy_streak >= 3 { 100 } else { 65 };
            let next_b = (cur_b as i64 + i64::from(st.probe_dir) * step).clamp(200, 4_000) as u64;
            let next_c = (cur_c + 75).min(2_200);
            self.batch_scale_permille.store(next_b, Ordering::Relaxed);
            self.concurrency_scale_permille
                .store(next_c, Ordering::Relaxed);
            if full_batches || st.prev_goodput_rec_per_sec <= 0.0 {
                st.prev_goodput_rec_per_sec = goodput_rps;
            }
        }
    }
}

/// High-throughput MPP Write Engine with global backpressure semaphore, adaptive batch/wait heuristic,
/// and smart per-key retries.
pub struct MppWriteEngine {
    sink: Arc<dyn AerospikeSink>,
    writer_config: WriterConfig,
    retry_config: RetryConfig,
    in_flight_semaphore: Arc<Semaphore>,
    metrics: Arc<ProxyMetrics>,
    tuner: AdaptiveBatchTuner,
}

impl MppWriteEngine {
    pub fn new(
        sink: Arc<dyn AerospikeSink>,
        writer_config: WriterConfig,
        retry_config: RetryConfig,
        metrics: Arc<ProxyMetrics>,
    ) -> Self {
        let max_in_flight = writer_config.max_in_flight_records.max(1);
        let tuner = AdaptiveBatchTuner::new(&writer_config);
        Self {
            sink,
            writer_config,
            retry_config,
            in_flight_semaphore: Arc::new(Semaphore::new(max_in_flight)),
            metrics,
            tuner,
        }
    }

    pub fn is_connected(&self) -> bool {
        self.sink.is_connected()
    }

    /// Returns a live diagnostic snapshot of the adaptive write batching controller.
    pub fn adaptive_snapshot(&self) -> AdaptiveTunerSnapshot {
        self.tuner.snapshot()
    }

    /// Stream-friendly zero-copy `RecordBatch` writer that dynamically adapts sub-batch size,
    /// concurrency, and micro-pacing on the fly to maximize RPS and avoid Aerospike cluster wait.
    pub async fn write_record_batch(
        &self,
        batch: &RecordBatch,
        params: &WriteParameters,
        max_bin_name_len: usize,
    ) -> Result<BatchWriteStats> {
        let total_rows = batch.num_rows();
        if total_rows == 0 {
            return Ok(BatchWriteStats::default());
        }

        if params.is_validate_mode() {
            return self
                .validate_record_batch(batch, params, max_bin_name_len)
                .await;
        }

        let key_col_idx = validate_record_batch_schema(batch, params, max_bin_name_len)?;
        let num_bins = batch.num_columns().saturating_sub(1).max(1);
        let approx_row_bytes = batch.get_array_memory_size() / total_rows.max(1);

        let use_batch = params
            .use_batch_write
            .unwrap_or(self.writer_config.use_batch_write);

        let mut in_flight_futures = FuturesUnordered::new();
        let mut stats = BatchWriteStats::default();
        let mut offset = 0usize;

        while offset < total_rows {
            let active_streams =
                self.metrics.active_streams.load(Ordering::Relaxed).max(1) as usize;
            let plan = self.tuner.plan(
                total_rows - offset,
                num_bins,
                approx_row_bytes,
                active_streams,
                &self.metrics,
            );
            let len = (total_rows - offset).min(plan.chunk_size.max(1));
            let slice = batch.slice(offset, len);
            offset += len;

            while in_flight_futures.len() >= plan.max_concurrency {
                let sub_stats: Result<(usize, u64, u64)> = in_flight_futures.next().await.unwrap();
                let (written, ov_ret, oth_ret) = sub_stats?;
                stats.rows_written += written;
                stats.overload_retries += ov_ret;
                stats.other_retries += oth_ret;
                tokio::task::yield_now().await;
            }

            let fut = self.write_subbatch_slice(
                slice,
                params.clone(),
                use_batch,
                key_col_idx,
                num_bins,
                plan.row_permit_weight,
                plan.pacing_delay_micros,
            );
            in_flight_futures.push(fut);
        }

        while let Some(res) = in_flight_futures.next().await {
            let (written, ov_ret, oth_ret) = res?;
            stats.rows_written += written;
            stats.overload_retries += ov_ret;
            stats.other_retries += oth_ret;
            tokio::task::yield_now().await;
        }

        self.metrics.inc_records_written(stats.rows_written as u64);
        self.metrics.inc_batches_processed();

        Ok(stats)
    }

    /// Stream-friendly zero-copy `RecordBatch` DQ validator that slices wide batches and
    /// acquires bin-weighted backpressure permits *before* allocating `PreparedRecord` structs.
    pub async fn validate_record_batch(
        &self,
        batch: &RecordBatch,
        params: &WriteParameters,
        max_bin_name_len: usize,
    ) -> Result<BatchWriteStats> {
        let total_rows = batch.num_rows();
        if total_rows == 0 {
            return Ok(BatchWriteStats::default());
        }

        let key_col_idx = validate_record_batch_schema(batch, params, max_bin_name_len)?;
        let num_bins = batch.num_columns().saturating_sub(1).max(1);
        let bin_weight = ((num_bins + 9) / 10).max(1);
        let base_chunk_size = self.writer_config.batch_write_size.max(1);
        let chunk_size = (base_chunk_size / bin_weight).max(16).min(base_chunk_size);
        let max_concurrency = self
            .writer_config
            .max_concurrent_subbatches_per_stream
            .max(1);

        let mut in_flight_futures = FuturesUnordered::new();
        let mut stats = BatchWriteStats::default();
        let mut offset = 0usize;

        while offset < total_rows {
            let len = (total_rows - offset).min(chunk_size);
            let slice = batch.slice(offset, len);
            offset += len;

            while in_flight_futures.len() >= max_concurrency {
                let sub_stats: Result<BatchWriteStats> = in_flight_futures.next().await.unwrap();
                let sub = sub_stats?;
                stats.rows_validated += sub.rows_validated;
                Self::merge_validation_stats(&mut stats, sub);
                tokio::task::yield_now().await;
            }

            let fut = self.validate_subbatch_slice(slice, params.clone(), key_col_idx, bin_weight);
            in_flight_futures.push(fut);
        }

        while let Some(res) = in_flight_futures.next().await {
            let sub = res?;
            stats.rows_validated += sub.rows_validated;
            Self::merge_validation_stats(&mut stats, sub);
            tokio::task::yield_now().await;
        }

        self.metrics
            .inc_records_validated(stats.rows_validated as u64);
        self.metrics
            .inc_validation_mismatches(stats.mismatched_rows as u64);
        self.metrics.inc_batches_processed();
        Ok(stats)
    }

    async fn write_subbatch_slice(
        &self,
        slice: RecordBatch,
        params: WriteParameters,
        use_batch: bool,
        key_col_idx: usize,
        num_bins: usize,
        row_permit_weight: usize,
        pacing_delay_micros: u64,
    ) -> Result<(usize, u64, u64)> {
        let slice_rows = slice.num_rows();
        let weighted_permits = slice_rows
            .saturating_mul(row_permit_weight.max(1))
            .min(self.writer_config.max_in_flight_records.max(1))
            .max(1) as u32;

        // 1. Acquire backpressure permits BEFORE converting the RecordBatch slice into heap-allocated Bins
        let sem_start = Instant::now();
        let _permit = self
            .in_flight_semaphore
            .acquire_many(weighted_permits)
            .await
            .map_err(|_| ProxyError::Internal("Backpressure semaphore closed".to_string()))?;
        let sem_wait_us = sem_start.elapsed().as_micros() as u64;

        // 2. Smooth bursty dispatches when Aerospike cluster queue-wait / overload pacing is active
        if pacing_delay_micros > 0 {
            tokio::time::sleep(Duration::from_micros(pacing_delay_micros)).await;
        }

        // 3. Convert only this small slice while holding the semaphore permit, then drop the slice reference
        let chunk = convert_record_batch_with_key_index(&slice, &params, key_col_idx)?;
        drop(slice);
        let num_records = chunk.len();
        if num_records == 0 {
            return Ok((0, 0, 0));
        }

        self.metrics.add_in_flight(num_records as i64);
        let start = Instant::now();

        let res = if use_batch {
            self.execute_batch_with_retry(chunk, &params, num_bins, sem_wait_us)
                .await
        } else {
            self.execute_single_puts_with_retry(chunk, &params, num_bins, sem_wait_us)
                .await
        };

        let elapsed_micros = start.elapsed().as_micros() as u64;
        self.metrics.observe_write_latency_micros(elapsed_micros);
        self.metrics.add_in_flight(-(num_records as i64));

        let (ov_ret, oth_ret) = res?;
        Ok((num_records, ov_ret, oth_ret))
    }

    async fn validate_subbatch_slice(
        &self,
        slice: RecordBatch,
        params: WriteParameters,
        key_col_idx: usize,
        bin_weight: usize,
    ) -> Result<BatchWriteStats> {
        let slice_rows = slice.num_rows();
        let weighted_permits = slice_rows
            .saturating_mul(bin_weight)
            .min(self.writer_config.max_in_flight_records.max(1))
            .max(1) as u32;

        let _permit = self
            .in_flight_semaphore
            .acquire_many(weighted_permits)
            .await
            .map_err(|_| ProxyError::Internal("Backpressure semaphore closed".to_string()))?;

        let chunk = convert_record_batch_with_key_index(&slice, &params, key_col_idx)?;
        drop(slice);
        let num_records = chunk.len();
        if num_records == 0 {
            return Ok(BatchWriteStats::default());
        }

        self.metrics.add_in_flight(num_records as i64);
        let start = Instant::now();

        let res = self.execute_batch_read_with_retry(chunk, &params).await;

        let elapsed_micros = start.elapsed().as_micros() as u64;
        self.metrics.observe_read_latency_micros(elapsed_micros);
        self.metrics.add_in_flight(-(num_records as i64));

        res
    }

    /// Write or validate a vector of `PreparedRecord`s concurrently in adaptive MPP sub-batches.
    pub async fn write_records(
        &self,
        records: Vec<PreparedRecord>,
        params: &WriteParameters,
    ) -> Result<BatchWriteStats> {
        if params.is_validate_mode() {
            return self.validate_records(records, params).await;
        }

        let total_rows = records.len();
        if total_rows == 0 {
            return Ok(BatchWriteStats::default());
        }

        let use_batch = params
            .use_batch_write
            .unwrap_or(self.writer_config.use_batch_write);
        let num_bins = records[0].bins.len().max(1);
        let approx_row_bytes = estimate_prepared_record_bytes(&records[0]);

        let mut iter = records.into_iter();
        let mut remaining = total_rows;
        let mut in_flight_futures = FuturesUnordered::new();
        let mut stats = BatchWriteStats {
            rows_written: total_rows,
            ..BatchWriteStats::default()
        };

        while remaining > 0 {
            let active_streams =
                self.metrics.active_streams.load(Ordering::Relaxed).max(1) as usize;
            let plan = self.tuner.plan(
                remaining,
                num_bins,
                approx_row_bytes,
                active_streams,
                &self.metrics,
            );
            let take_n = remaining.min(plan.chunk_size.max(1));
            let chunk: Vec<PreparedRecord> = iter.by_ref().take(take_n).collect();
            if chunk.is_empty() {
                break;
            }
            remaining = remaining.saturating_sub(chunk.len());

            while in_flight_futures.len() >= plan.max_concurrency {
                let sub_stats: Result<(u64, u64)> = in_flight_futures.next().await.unwrap();
                let (ov_ret, oth_ret) = sub_stats?;
                stats.overload_retries += ov_ret;
                stats.other_retries += oth_ret;
            }

            let fut = self.write_subbatch(
                chunk,
                params.clone(),
                use_batch,
                num_bins,
                plan.row_permit_weight,
                plan.pacing_delay_micros,
            );
            in_flight_futures.push(fut);
        }

        while let Some(res) = in_flight_futures.next().await {
            let (ov_ret, oth_ret) = res?;
            stats.overload_retries += ov_ret;
            stats.other_retries += oth_ret;
        }

        self.metrics.inc_records_written(total_rows as u64);
        self.metrics.inc_batches_processed();

        Ok(stats)
    }

    /// Data Quality (DQ) validation: read records in parallel sub-batches from Aerospike
    /// and compare each record's bins against the incoming `PreparedRecord` (with `NaN == NaN` as `true`).
    pub async fn validate_records(
        &self,
        records: Vec<PreparedRecord>,
        params: &WriteParameters,
    ) -> Result<BatchWriteStats> {
        let total_rows = records.len();
        if total_rows == 0 {
            return Ok(BatchWriteStats::default());
        }

        let chunk_size = self.writer_config.batch_write_size.max(1);
        let max_concurrency = self
            .writer_config
            .max_concurrent_subbatches_per_stream
            .max(1);

        let chunks = split_into_chunks(records, chunk_size);

        let mut in_flight_futures = FuturesUnordered::new();
        let mut stats = BatchWriteStats {
            rows_validated: total_rows,
            ..BatchWriteStats::default()
        };

        for chunk in chunks {
            while in_flight_futures.len() >= max_concurrency {
                let sub_stats: Result<BatchWriteStats> = in_flight_futures.next().await.unwrap();
                let sub = sub_stats?;
                Self::merge_validation_stats(&mut stats, sub);
            }

            let fut = self.validate_subbatch(chunk, params.clone());
            in_flight_futures.push(fut);
        }

        while let Some(res) = in_flight_futures.next().await {
            let sub = res?;
            Self::merge_validation_stats(&mut stats, sub);
        }

        self.metrics
            .inc_records_validated(stats.rows_validated as u64);
        self.metrics
            .inc_validation_mismatches(stats.mismatched_rows as u64);
        self.metrics.inc_batches_processed();
        Ok(stats)
    }

    fn merge_validation_stats(acc: &mut BatchWriteStats, sub: BatchWriteStats) {
        acc.overload_retries += sub.overload_retries;
        acc.other_retries += sub.other_retries;
        acc.matched_rows += sub.matched_rows;
        acc.mismatched_rows += sub.mismatched_rows;
        acc.missing_keys += sub.missing_keys;
        acc.bin_mismatch_rows += sub.bin_mismatch_rows;
        for sample in sub.mismatch_samples {
            if acc.mismatch_samples.len() < 20 {
                acc.mismatch_samples.push(sample);
            }
        }
    }

    async fn validate_subbatch(
        &self,
        chunk: Vec<PreparedRecord>,
        params: WriteParameters,
    ) -> Result<BatchWriteStats> {
        let num_records = chunk.len();
        let permit_count = num_records.min(self.writer_config.max_in_flight_records.max(1)) as u32;

        let _permit = self
            .in_flight_semaphore
            .acquire_many(permit_count)
            .await
            .map_err(|_| ProxyError::Internal("Backpressure semaphore closed".to_string()))?;

        self.metrics.add_in_flight(num_records as i64);
        let start = Instant::now();

        let res = self.execute_batch_read_with_retry(chunk, &params).await;

        let elapsed_micros = start.elapsed().as_micros() as u64;
        self.metrics.observe_read_latency_micros(elapsed_micros);
        self.metrics.add_in_flight(-(num_records as i64));

        res
    }

    async fn execute_batch_read_with_retry(
        &self,
        initial_records: Vec<PreparedRecord>,
        params: &WriteParameters,
    ) -> Result<BatchWriteStats> {
        let mut pending_records: Vec<PreparedRecord> = initial_records;
        let mut pending_attempts: Vec<(u32, u32)> = vec![(0, 0); pending_records.len()];

        let mut stats = BatchWriteStats {
            rows_validated: pending_records.len(),
            ..BatchWriteStats::default()
        };
        let mut backoff_step: u32 = 0;

        while !pending_records.is_empty() {
            let outcomes = self.sink.execute_batch_read(&pending_records, params).await;

            let mut next_records = Vec::new();
            let mut next_attempts = Vec::new();
            let mut batch_overload_count: u64 = 0;
            let mut batch_other_count: u64 = 0;

            for ((record, (mut ov_att, mut oth_att)), outcome) in pending_records
                .into_iter()
                .zip(pending_attempts.into_iter())
                .zip(outcomes.into_iter())
            {
                match outcome {
                    RecordReadOutcome::Found(actual_bins) => {
                        match validate_record_against_bins(&record, Some(&actual_bins)) {
                            RecordDqResult::Matched => {
                                stats.matched_rows += 1;
                            }
                            RecordDqResult::MissingKey => {
                                stats.missing_keys += 1;
                                stats.mismatched_rows += 1;
                                if stats.mismatch_samples.len() < 20 {
                                    stats.mismatch_samples.push(DqMismatchSample {
                                        key: format!("{:?}", record.key),
                                        reason: "key_not_found".to_string(),
                                    });
                                }
                            }
                            RecordDqResult::BinMismatch(reason) => {
                                stats.bin_mismatch_rows += 1;
                                stats.mismatched_rows += 1;
                                if stats.mismatch_samples.len() < 20 {
                                    stats.mismatch_samples.push(DqMismatchSample {
                                        key: format!("{:?}", record.key),
                                        reason,
                                    });
                                }
                            }
                        }
                    }
                    RecordReadOutcome::NotFound => {
                        stats.missing_keys += 1;
                        stats.mismatched_rows += 1;
                        if stats.mismatch_samples.len() < 20 {
                            stats.mismatch_samples.push(DqMismatchSample {
                                key: format!("{:?}", record.key),
                                reason: "key_not_found".to_string(),
                            });
                        }
                    }
                    RecordReadOutcome::OverloadError(err_msg) => {
                        ov_att = ov_att.saturating_add(1);
                        batch_overload_count += 1;
                        debug!(
                            key = ?record.key,
                            overload_attempt = ov_att,
                            error = %err_msg,
                            "Overload error on DQ batch read; scheduling infinite backoff retry"
                        );
                        next_records.push(record);
                        next_attempts.push((ov_att, oth_att));
                    }
                    RecordReadOutcome::OtherError(err_msg) => {
                        oth_att = oth_att.saturating_add(1);
                        if oth_att > self.retry_config.max_other_retries {
                            self.metrics.inc_failed_records(1);
                            return Err(ProxyError::WriteFailed {
                                attempts: oth_att,
                                message: err_msg,
                            });
                        }
                        batch_other_count += 1;
                        next_records.push(record);
                        next_attempts.push((ov_att, oth_att));
                    }
                }
            }

            if next_records.is_empty() {
                break;
            }

            if batch_overload_count > 0 {
                stats.overload_retries += batch_overload_count;
                self.metrics.inc_overload_retries(batch_overload_count);
            }
            if batch_other_count > 0 {
                stats.other_retries += batch_other_count;
                self.metrics.inc_other_retries(batch_other_count);
            }

            let sleep_dur = compute_backoff_with_jitter(&self.retry_config, backoff_step);
            backoff_step = backoff_step.saturating_add(1);
            tokio::time::sleep(sleep_dur).await;

            pending_records = next_records;
            pending_attempts = next_attempts;
        }

        Ok(stats)
    }

    async fn write_subbatch(
        &self,
        chunk: Vec<PreparedRecord>,
        params: WriteParameters,
        use_batch: bool,
        num_bins: usize,
        row_permit_weight: usize,
        pacing_delay_micros: u64,
    ) -> Result<(u64, u64)> {
        let num_records = chunk.len();
        let permit_count = num_records
            .saturating_mul(row_permit_weight.max(1))
            .min(self.writer_config.max_in_flight_records.max(1))
            .max(1) as u32;

        // Acquire backpressure permits before dispatching to Aerospike
        let sem_start = Instant::now();
        let _permit = self
            .in_flight_semaphore
            .acquire_many(permit_count)
            .await
            .map_err(|_| ProxyError::Internal("Backpressure semaphore closed".to_string()))?;
        let sem_wait_us = sem_start.elapsed().as_micros() as u64;

        if pacing_delay_micros > 0 {
            tokio::time::sleep(Duration::from_micros(pacing_delay_micros)).await;
        }

        self.metrics.add_in_flight(num_records as i64);
        let start = Instant::now();

        let res = if use_batch {
            self.execute_batch_with_retry(chunk, &params, num_bins, sem_wait_us)
                .await
        } else {
            self.execute_single_puts_with_retry(chunk, &params, num_bins, sem_wait_us)
                .await
        };

        let elapsed_micros = start.elapsed().as_micros() as u64;
        self.metrics.observe_write_latency_micros(elapsed_micros);
        self.metrics.add_in_flight(-(num_records as i64));

        res
    }

    /// Execute a sub-batch using `AerospikeSink::execute_batch_write` with per-key retry tracking
    /// and closed-loop telemetry feedback into `AdaptiveBatchTuner`:
    /// - Fast zero-allocation exit when all keys succeed on the first RPC.
    /// - Keys that fail with `OverloadError` retry indefinitely ("до талого") with exponential backoff,
    ///   jitter, and adaptive retry sub-batch splitting to drain hot partitions without cluster wait.
    /// - Keys that fail with `OtherError` increment their per-key `other_attempts` counter and terminate
    ///   the stream if `other_attempts > max_other_retries`.
    async fn execute_batch_with_retry(
        &self,
        initial_records: Vec<PreparedRecord>,
        params: &WriteParameters,
        num_bins: usize,
        sem_wait_us: u64,
    ) -> Result<(u64, u64)> {
        if initial_records.is_empty() {
            return Ok((0, 0));
        }

        // Round 0 fast path: avoid allocating `pending_attempts` when all keys succeed immediately.
        let rpc_start = Instant::now();
        let first_outcomes = self
            .sink
            .execute_batch_write(&initial_records, params)
            .await;
        let first_lat_us = rpc_start.elapsed().as_micros() as u64;

        if first_outcomes
            .iter()
            .all(|o| matches!(o, RecordWriteOutcome::Success))
        {
            self.tuner.observe_rpc(
                initial_records.len(),
                num_bins,
                first_lat_us,
                0,
                sem_wait_us,
                &self.metrics,
            );
            return Ok((0, 0));
        }

        let initial_len = initial_records.len();
        let mut pending_records = Vec::new();
        let mut pending_attempts: Vec<(u32, u32)> = Vec::new();
        let mut total_overload_retries: u64 = 0;
        let mut total_other_retries: u64 = 0;
        let mut first_overload_count: u64 = 0;
        let mut first_other_count: u64 = 0;

        for (record, outcome) in initial_records.into_iter().zip(first_outcomes) {
            match outcome {
                RecordWriteOutcome::Success => {}
                RecordWriteOutcome::OverloadError(err_msg) => {
                    first_overload_count += 1;
                    debug!(
                        key = ?record.key,
                        overload_attempt = 1,
                        error = %err_msg,
                        "Overload error on key; scheduling infinite backoff retry"
                    );
                    pending_records.push(record);
                    pending_attempts.push((1, 0));
                }
                RecordWriteOutcome::OtherError(err_msg) => {
                    if 1 > self.retry_config.max_other_retries {
                        self.metrics.inc_failed_records(1);
                        warn!(
                            key = ?record.key,
                            attempts = 1,
                            error = %err_msg,
                            "Non-overload error exceeded max_other_retries; terminating stream"
                        );
                        return Err(ProxyError::WriteFailed {
                            attempts: 1,
                            message: err_msg,
                        });
                    }
                    first_other_count += 1;
                    warn!(
                        key = ?record.key,
                        other_attempt = 1,
                        max_other_retries = self.retry_config.max_other_retries,
                        error = %err_msg,
                        "Non-overload error on key; retrying"
                    );
                    pending_records.push(record);
                    pending_attempts.push((0, 1));
                }
            }
        }

        self.tuner.observe_rpc(
            initial_len,
            num_bins,
            first_lat_us,
            first_overload_count,
            sem_wait_us,
            &self.metrics,
        );

        if first_overload_count > 0 {
            total_overload_retries += first_overload_count;
            self.metrics.inc_overload_retries(first_overload_count);
        }
        if first_other_count > 0 {
            total_other_retries += first_other_count;
            self.metrics.inc_other_retries(first_other_count);
        }

        let mut backoff_step: u32 = 0;
        while !pending_records.is_empty() {
            let sleep_dur = compute_backoff_with_jitter(&self.retry_config, backoff_step);
            backoff_step = backoff_step.saturating_add(1);
            tokio::time::sleep(sleep_dur).await;

            // Split retry batch into gentler adaptive slices so overloaded Aerospike partitions drain cleanly
            let retry_cap = self
                .tuner
                .retry_chunk_size(pending_records.len(), backoff_step);

            let mut next_records = Vec::new();
            let mut next_attempts = Vec::new();
            let mut round_overload_count: u64 = 0;
            let mut round_other_count: u64 = 0;

            let mut rec_iter = pending_records.into_iter();
            let mut att_iter = pending_attempts.into_iter();

            loop {
                let sub_recs: Vec<PreparedRecord> = rec_iter.by_ref().take(retry_cap).collect();
                if sub_recs.is_empty() {
                    break;
                }
                let sub_len = sub_recs.len();
                let sub_atts: Vec<(u32, u32)> = att_iter.by_ref().take(sub_len).collect();

                let sub_start = Instant::now();
                let outcomes = self.sink.execute_batch_write(&sub_recs, params).await;
                let sub_lat_us = sub_start.elapsed().as_micros() as u64;
                let mut sub_ov: u64 = 0;

                for ((record, (mut ov_att, mut oth_att)), outcome) in
                    sub_recs.into_iter().zip(sub_atts).zip(outcomes)
                {
                    match outcome {
                        RecordWriteOutcome::Success => {}
                        RecordWriteOutcome::OverloadError(err_msg) => {
                            ov_att = ov_att.saturating_add(1);
                            sub_ov += 1;
                            round_overload_count += 1;
                            debug!(
                                key = ?record.key,
                                overload_attempt = ov_att,
                                error = %err_msg,
                                "Overload error on key; scheduling infinite backoff retry"
                            );
                            next_records.push(record);
                            next_attempts.push((ov_att, oth_att));
                        }
                        RecordWriteOutcome::OtherError(err_msg) => {
                            oth_att = oth_att.saturating_add(1);
                            if oth_att > self.retry_config.max_other_retries {
                                self.metrics.inc_failed_records(1);
                                warn!(
                                    key = ?record.key,
                                    attempts = oth_att,
                                    error = %err_msg,
                                    "Non-overload error exceeded max_other_retries; terminating stream"
                                );
                                return Err(ProxyError::WriteFailed {
                                    attempts: oth_att,
                                    message: err_msg,
                                });
                            }
                            round_other_count += 1;
                            warn!(
                                key = ?record.key,
                                other_attempt = oth_att,
                                max_other_retries = self.retry_config.max_other_retries,
                                error = %err_msg,
                                "Non-overload error on key; retrying"
                            );
                            next_records.push(record);
                            next_attempts.push((ov_att, oth_att));
                        }
                    }
                }

                self.tuner
                    .observe_rpc(sub_len, num_bins, sub_lat_us, sub_ov, 0, &self.metrics);
            }

            if round_overload_count > 0 {
                total_overload_retries += round_overload_count;
                self.metrics.inc_overload_retries(round_overload_count);
            }
            if round_other_count > 0 {
                total_other_retries += round_other_count;
                self.metrics.inc_other_retries(round_other_count);
            }

            pending_records = next_records;
            pending_attempts = next_attempts;
        }

        Ok((total_overload_retries, total_other_retries))
    }

    /// Fallback single-record `put` execution with the same retry semantics.
    async fn execute_single_puts_with_retry(
        &self,
        records: Vec<PreparedRecord>,
        params: &WriteParameters,
        num_bins: usize,
        sem_wait_us: u64,
    ) -> Result<(u64, u64)> {
        let count = records.len();
        let sub_start = Instant::now();
        let mut futs = FuturesUnordered::new();
        for record in records {
            futs.push(self.execute_one_put_with_retry(record, params));
        }

        let mut total_overload_retries: u64 = 0;
        let mut total_other_retries: u64 = 0;

        while let Some(res) = futs.next().await {
            let (ov, oth) = res?;
            total_overload_retries += ov;
            total_other_retries += oth;
        }

        let lat_us = sub_start.elapsed().as_micros() as u64;
        self.tuner.observe_rpc(
            count,
            num_bins,
            lat_us,
            total_overload_retries,
            sem_wait_us,
            &self.metrics,
        );

        Ok((total_overload_retries, total_other_retries))
    }

    async fn execute_one_put_with_retry(
        &self,
        record: PreparedRecord,
        params: &WriteParameters,
    ) -> Result<(u64, u64)> {
        let mut overload_attempts: u64 = 0;
        let mut other_attempts: u32 = 0;
        let mut backoff_step: u32 = 0;

        loop {
            match self.sink.execute_single_put(&record, params).await {
                RecordWriteOutcome::Success => {
                    return Ok((overload_attempts, u64::from(other_attempts)));
                }
                RecordWriteOutcome::OverloadError(err_msg) => {
                    overload_attempts += 1;
                    self.metrics.inc_overload_retries(1);
                    debug!(
                        key = ?record.key,
                        overload_attempts,
                        error = %err_msg,
                        "Single put overload error; retrying with backoff"
                    );
                }
                RecordWriteOutcome::OtherError(err_msg) => {
                    other_attempts = other_attempts.saturating_add(1);
                    if other_attempts > self.retry_config.max_other_retries {
                        self.metrics.inc_failed_records(1);
                        return Err(ProxyError::WriteFailed {
                            attempts: other_attempts,
                            message: err_msg,
                        });
                    }
                    self.metrics.inc_other_retries(1);
                }
            }

            let sleep_dur = compute_backoff_with_jitter(&self.retry_config, backoff_step);
            backoff_step = backoff_step.saturating_add(1);
            tokio::time::sleep(sleep_dur).await;
        }
    }

    /// Stream records from a contiguous range of Aerospike partitions `[begin_partition, begin_partition + partition_count)`.
    pub async fn scan_partition_range(
        &self,
        namespace: &str,
        set_name: &str,
        begin_partition: usize,
        partition_count: usize,
        bins: Option<Vec<String>>,
        max_records: Option<u64>,
    ) -> Result<mpsc::Receiver<Result<ScannedRecord>>> {
        self.sink
            .scan_partition_range(
                namespace,
                set_name,
                begin_partition,
                partition_count,
                bins,
                max_records,
            )
            .await
    }

    /// Read a batch of keys from Aerospike in parallel sub-batches with overload/error retry,
    /// preserving the exact input order of `keys`.
    pub async fn read_keys(
        &self,
        keys: Vec<Key>,
        bins: Option<Vec<String>>,
    ) -> Result<Vec<RecordReadOutcome>> {
        let shared_bins = bins.map(Arc::<[String]>::from);
        self.read_keys_slice(&keys, shared_bins).await
    }

    /// Read a slice of keys from Aerospike in parallel sub-batches without cloning `keys` or `bins`
    /// on the happy path (cloning only failed keys if a transient retry is required).
    pub async fn read_keys_slice(
        &self,
        keys: &[Key],
        bins: Option<Arc<[String]>>,
    ) -> Result<Vec<RecordReadOutcome>> {
        let total = keys.len();
        if total == 0 {
            return Ok(Vec::new());
        }

        let chunk_size = self.writer_config.batch_write_size.max(1);
        let max_concurrency = self
            .writer_config
            .max_concurrent_subbatches_per_stream
            .max(1);

        let chunks: Vec<&[Key]> = keys.chunks(chunk_size).collect();
        let mut ordered_chunks: Vec<Option<Vec<RecordReadOutcome>>> = vec![None; chunks.len()];
        let mut in_flight = FuturesUnordered::new();

        for (chunk_idx, chunk_keys) in chunks.into_iter().enumerate() {
            while in_flight.len() >= max_concurrency {
                let res: Result<(usize, Vec<RecordReadOutcome>)> = in_flight.next().await.unwrap();
                let (done_idx, outcomes) = res?;
                ordered_chunks[done_idx] = Some(outcomes);
            }
            let bins_clone = bins.clone();
            in_flight.push(self.read_keys_subbatch(chunk_idx, chunk_keys, bins_clone));
        }

        while let Some(res) = in_flight.next().await {
            let (done_idx, outcomes) = res?;
            ordered_chunks[done_idx] = Some(outcomes);
        }

        let mut all_outcomes = Vec::with_capacity(total);
        for chunk_opt in ordered_chunks {
            if let Some(mut c) = chunk_opt {
                all_outcomes.append(&mut c);
            }
        }
        Ok(all_outcomes)
    }

    async fn read_keys_subbatch<'a>(
        &'a self,
        chunk_idx: usize,
        keys: &'a [Key],
        bins: Option<Arc<[String]>>,
    ) -> Result<(usize, Vec<RecordReadOutcome>)> {
        let num_records = keys.len();
        let permit_count = num_records.min(self.writer_config.max_in_flight_records.max(1)) as u32;

        let _permit = self
            .in_flight_semaphore
            .acquire_many(permit_count)
            .await
            .map_err(|_| ProxyError::Internal("Backpressure semaphore closed".to_string()))?;

        self.metrics.add_in_flight(num_records as i64);
        let start = Instant::now();

        let res = self
            .execute_key_reads_with_retry(keys, bins.as_deref())
            .await;

        let elapsed_micros = start.elapsed().as_micros() as u64;
        self.metrics.observe_read_latency_micros(elapsed_micros);
        self.metrics.add_in_flight(-(num_records as i64));

        res.map(|outcomes| (chunk_idx, outcomes))
    }

    async fn execute_key_reads_with_retry(
        &self,
        initial_keys: &[Key],
        bins: Option<&[String]>,
    ) -> Result<Vec<RecordReadOutcome>> {
        let total = initial_keys.len();
        let first_outcomes = self.sink.execute_batch_read_keys(initial_keys, bins).await;
        if first_outcomes
            .iter()
            .all(|o| matches!(o, RecordReadOutcome::Found(_) | RecordReadOutcome::NotFound))
        {
            return Ok(first_outcomes);
        }

        let mut results: Vec<Option<RecordReadOutcome>> = vec![None; total];
        let mut pending_keys: Vec<Key> = Vec::new();
        let mut pending_meta: Vec<(usize, u32, u32)> = Vec::new();
        let mut init_overload_count: u64 = 0;
        let mut init_other_count: u64 = 0;

        for (orig_idx, (key, outcome)) in initial_keys
            .iter()
            .zip(first_outcomes.into_iter())
            .enumerate()
        {
            match outcome {
                RecordReadOutcome::Found(_) | RecordReadOutcome::NotFound => {
                    results[orig_idx] = Some(outcome);
                }
                RecordReadOutcome::OverloadError(_) => {
                    init_overload_count += 1;
                    pending_keys.push(key.clone());
                    pending_meta.push((orig_idx, 1, 0));
                }
                RecordReadOutcome::OtherError(err_msg) => {
                    if 1 > self.retry_config.max_other_retries {
                        self.metrics.inc_failed_records(1);
                        return Err(ProxyError::WriteFailed {
                            attempts: 1,
                            message: err_msg,
                        });
                    }
                    init_other_count += 1;
                    pending_keys.push(key.clone());
                    pending_meta.push((orig_idx, 0, 1));
                }
            }
        }

        if init_overload_count > 0 {
            self.metrics.inc_overload_retries(init_overload_count);
        }
        if init_other_count > 0 {
            self.metrics.inc_other_retries(init_other_count);
        }

        let mut backoff_step: u32 = 0;
        if !pending_keys.is_empty() {
            let sleep_dur = compute_backoff_with_jitter(&self.retry_config, backoff_step);
            backoff_step = backoff_step.saturating_add(1);
            tokio::time::sleep(sleep_dur).await;
        }

        while !pending_keys.is_empty() {
            let outcomes = self.sink.execute_batch_read_keys(&pending_keys, bins).await;

            let mut next_keys = Vec::new();
            let mut next_meta = Vec::new();
            let mut batch_overload_count: u64 = 0;
            let mut batch_other_count: u64 = 0;

            for ((key, (orig_idx, mut ov_att, mut oth_att)), outcome) in pending_keys
                .into_iter()
                .zip(pending_meta.into_iter())
                .zip(outcomes.into_iter())
            {
                match outcome {
                    RecordReadOutcome::Found(_) | RecordReadOutcome::NotFound => {
                        results[orig_idx] = Some(outcome);
                    }
                    RecordReadOutcome::OverloadError(_) => {
                        ov_att = ov_att.saturating_add(1);
                        batch_overload_count += 1;
                        next_keys.push(key);
                        next_meta.push((orig_idx, ov_att, oth_att));
                    }
                    RecordReadOutcome::OtherError(err_msg) => {
                        oth_att = oth_att.saturating_add(1);
                        if oth_att > self.retry_config.max_other_retries {
                            self.metrics.inc_failed_records(1);
                            return Err(ProxyError::WriteFailed {
                                attempts: oth_att,
                                message: err_msg,
                            });
                        }
                        batch_other_count += 1;
                        next_keys.push(key);
                        next_meta.push((orig_idx, ov_att, oth_att));
                    }
                }
            }

            if next_keys.is_empty() {
                break;
            }

            if batch_overload_count > 0 {
                self.metrics.inc_overload_retries(batch_overload_count);
            }
            if batch_other_count > 0 {
                self.metrics.inc_other_retries(batch_other_count);
            }

            let sleep_dur = compute_backoff_with_jitter(&self.retry_config, backoff_step);
            backoff_step = backoff_step.saturating_add(1);
            tokio::time::sleep(sleep_dur).await;

            pending_keys = next_keys;
            pending_meta = next_meta;
        }

        Ok(results
            .into_iter()
            .map(|r| r.unwrap_or(RecordReadOutcome::NotFound))
            .collect())
    }
}

/// Estimate wire/heap footprint of a single `PreparedRecord` for adaptive batch byte-budgeting.
fn estimate_prepared_record_bytes(rec: &PreparedRecord) -> usize {
    let mut total = 64usize;
    for bin in &rec.bins {
        total = total.saturating_add(bin.name.len() + 16);
        total = total.saturating_add(match &bin.value {
            Value::String(s) => s.len(),
            Value::Blob(b) | Value::Hll(b) => b.len(),
            Value::List(l) => l.len().saturating_mul(16),
            Value::HashMap(m) => m.len().saturating_mul(32),
            Value::OrderedMap(m) => m.len().saturating_mul(32),
            _ => 8,
        });
    }
    total
}

/// Split an owned `Vec<T>` into sub-vectors of at most `chunk_size` elements without cloning `T`.
fn split_into_chunks<T>(items: Vec<T>, chunk_size: usize) -> Vec<Vec<T>> {
    let step = chunk_size.max(1);
    let num_chunks = items.len().div_ceil(step);
    let mut out = Vec::with_capacity(num_chunks);
    let mut iter = items.into_iter();
    loop {
        let chunk: Vec<T> = iter.by_ref().take(step).collect();
        if chunk.is_empty() {
            break;
        }
        out.push(chunk);
    }
    out
}

/// Compute exponential backoff with full jitter:
/// `cap = min(max_backoff_ms, initial_backoff_ms * multiplier^step)`
/// `sleep = uniform_random(1..=cap)`
pub fn compute_backoff_with_jitter(config: &RetryConfig, step: u32) -> Duration {
    let base = config.initial_backoff_ms.max(1) as f64;
    let max_ms = config.max_backoff_ms.max(config.initial_backoff_ms).max(1) as f64;
    let mult = if config.backoff_multiplier > 1.0 {
        config.backoff_multiplier
    } else {
        2.0
    };

    let factor = mult.powi(step.min(30) as i32);
    let cap_ms = (base * factor).min(max_ms) as u64;
    if cap_ms <= 1 {
        return Duration::from_millis(1);
    }

    let jittered_ms = rand::rng().random_range(1..=cap_ms);
    Duration::from_millis(jittered_ms)
}
