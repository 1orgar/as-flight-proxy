use aerospike::operations;
use aerospike::query::PartitionFilter;
use aerospike::{
    BatchOperation, BatchPolicy, BatchReadPolicy, BatchWritePolicy, Bins, Client,
    ClientResultCode, Concurrency, ErrorKind, Key, QueryPolicy, ResultCode, Statement, Value,
    WritePolicy,
};
use futures::stream::{FuturesUnordered, StreamExt};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Semaphore, mpsc};
use tracing::{debug, warn};

use crate::config::{RetryConfig, WriteParameters, WriterConfig};
use crate::converter::{
    PreparedRecord, RecordDqResult, ScannedRecord, convert_record_batch,
    validate_record_against_bins,
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
            let ops: Vec<operations::Operation> =
                rec.bins.iter().map(operations::put).collect();
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

        let all_nil =
            !record.bins.is_empty() && record.bins.iter().all(|b| b.value == Value::Nil);

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
            .map(|rec| BatchOperation::read(&read_policy, rec.key.clone(), Bins::All))
            .collect();

        match self.client.batch(&batch_policy, &mut batch_ops).await {
            Ok(()) => batch_ops
                .iter()
                .map(classify_batch_read_op)
                .collect(),
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
                .map(|r| {
                    r.bins
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect()
                })
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

/// High-throughput MPP Write Engine with global backpressure semaphore and smart per-key retries.
pub struct MppWriteEngine {
    sink: Arc<dyn AerospikeSink>,
    writer_config: WriterConfig,
    retry_config: RetryConfig,
    in_flight_semaphore: Arc<Semaphore>,
    metrics: Arc<ProxyMetrics>,
}

impl MppWriteEngine {
    pub fn new(
        sink: Arc<dyn AerospikeSink>,
        writer_config: WriterConfig,
        retry_config: RetryConfig,
        metrics: Arc<ProxyMetrics>,
    ) -> Self {
        let max_in_flight = writer_config.max_in_flight_records.max(1);
        Self {
            sink,
            writer_config,
            retry_config,
            in_flight_semaphore: Arc::new(Semaphore::new(max_in_flight)),
            metrics,
        }
    }

    pub fn is_connected(&self) -> bool {
        self.sink.is_connected()
    }

    /// Stream-friendly zero-copy `RecordBatch` writer that slices wide batches into sub-batches
    /// and acquires backpressure permits (weighted by bin column count) *before* allocating
    /// `PreparedRecord` structs on the heap.
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

        let num_bins = batch.num_columns().saturating_sub(1).max(1);
        let bin_weight = ((num_bins + 9) / 10).max(1);
        let base_chunk_size = self.writer_config.batch_write_size.max(1);
        let chunk_size = (base_chunk_size / bin_weight).max(16).min(base_chunk_size);

        let use_batch = params
            .use_batch_write
            .unwrap_or(self.writer_config.use_batch_write);
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
                max_bin_name_len,
                bin_weight,
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

            let fut =
                self.validate_subbatch_slice(slice, params.clone(), max_bin_name_len, bin_weight);
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
        max_bin_name_len: usize,
        bin_weight: usize,
    ) -> Result<(usize, u64, u64)> {
        let slice_rows = slice.num_rows();
        let weighted_permits = slice_rows
            .saturating_mul(bin_weight)
            .min(self.writer_config.max_in_flight_records.max(1))
            .max(1) as u32;

        // 1. Acquire backpressure permits BEFORE converting the RecordBatch slice into heap-allocated Bins
        let _permit = self
            .in_flight_semaphore
            .acquire_many(weighted_permits)
            .await
            .map_err(|_| ProxyError::Internal("Backpressure semaphore closed".to_string()))?;

        // 2. Convert only this small slice while holding the semaphore permit, then drop the slice reference
        let chunk = convert_record_batch(&slice, &params, max_bin_name_len)?;
        drop(slice);
        let num_records = chunk.len();
        if num_records == 0 {
            return Ok((0, 0, 0));
        }

        self.metrics.add_in_flight(num_records as i64);
        let start = Instant::now();

        let res = if use_batch {
            self.execute_batch_with_retry(chunk, &params).await
        } else {
            self.execute_single_puts_with_retry(chunk, &params).await
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
        max_bin_name_len: usize,
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

        let chunk = convert_record_batch(&slice, &params, max_bin_name_len)?;
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

    /// Write or validate a vector of `PreparedRecord`s concurrently in MPP sub-batches.
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
        let chunk_size = self.writer_config.batch_write_size.max(1);
        let max_concurrency = self
            .writer_config
            .max_concurrent_subbatches_per_stream
            .max(1);

        let chunks = split_into_chunks(records, chunk_size);

        let mut in_flight_futures = FuturesUnordered::new();
        let mut stats = BatchWriteStats {
            rows_written: total_rows,
            ..BatchWriteStats::default()
        };

        for chunk in chunks {
            while in_flight_futures.len() >= max_concurrency {
                let sub_stats: Result<(u64, u64)> = in_flight_futures.next().await.unwrap();
                let (ov_ret, oth_ret) = sub_stats?;
                stats.overload_retries += ov_ret;
                stats.other_retries += oth_ret;
            }

            let fut = self.write_subbatch(chunk, params.clone(), use_batch);
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
            let outcomes = self
                .sink
                .execute_batch_read(&pending_records, params)
                .await;

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
    ) -> Result<(u64, u64)> {
        let num_records = chunk.len();
        let permit_count = num_records.min(self.writer_config.max_in_flight_records.max(1)) as u32;

        // Acquire backpressure permits before dispatching to Aerospike
        let _permit = self
            .in_flight_semaphore
            .acquire_many(permit_count)
            .await
            .map_err(|_| ProxyError::Internal("Backpressure semaphore closed".to_string()))?;

        self.metrics.add_in_flight(num_records as i64);
        let start = Instant::now();

        let res = if use_batch {
            self.execute_batch_with_retry(chunk, &params).await
        } else {
            self.execute_single_puts_with_retry(chunk, &params).await
        };

        let elapsed_micros = start.elapsed().as_micros() as u64;
        self.metrics.observe_write_latency_micros(elapsed_micros);
        self.metrics.add_in_flight(-(num_records as i64));

        res
    }

    /// Execute a sub-batch using `AerospikeSink::execute_batch_write` with per-key retry tracking:
    /// - Keys that succeed (`ResultCode::Ok`) are immediately dropped from `pending`.
    /// - Keys that fail with `OverloadError` retry indefinitely ("до талого") with exponential backoff + jitter.
    /// - Keys that fail with `OtherError` increment their per-key `other_attempts` counter and terminate
    ///   the stream if `other_attempts > max_other_retries`.
    async fn execute_batch_with_retry(
        &self,
        initial_records: Vec<PreparedRecord>,
        params: &WriteParameters,
    ) -> Result<(u64, u64)> {
        let mut pending_records: Vec<PreparedRecord> = initial_records;
        let mut pending_attempts: Vec<(u32, u32)> = vec![(0, 0); pending_records.len()];

        let mut total_overload_retries: u64 = 0;
        let mut total_other_retries: u64 = 0;
        let mut backoff_step: u32 = 0;

        while !pending_records.is_empty() {
            let outcomes = self
                .sink
                .execute_batch_write(&pending_records, params)
                .await;

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
                    RecordWriteOutcome::Success => {
                        // Successfully written; do not re-queue.
                    }
                    RecordWriteOutcome::OverloadError(err_msg) => {
                        ov_att = ov_att.saturating_add(1);
                        batch_overload_count += 1;
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
                        batch_other_count += 1;
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

            if next_records.is_empty() {
                break;
            }

            if batch_overload_count > 0 {
                total_overload_retries += batch_overload_count;
                self.metrics.inc_overload_retries(batch_overload_count);
            }
            if batch_other_count > 0 {
                total_other_retries += batch_other_count;
                self.metrics.inc_other_retries(batch_other_count);
            }

            let sleep_dur = compute_backoff_with_jitter(&self.retry_config, backoff_step);
            backoff_step = backoff_step.saturating_add(1);
            tokio::time::sleep(sleep_dur).await;

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
    ) -> Result<(u64, u64)> {
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
        let total = keys.len();
        if total == 0 {
            return Ok(Vec::new());
        }

        let chunk_size = self.writer_config.batch_write_size.max(1);
        let max_concurrency = self
            .writer_config
            .max_concurrent_subbatches_per_stream
            .max(1);

        let raw_chunks = split_into_chunks(keys, chunk_size);
        let mut ordered_chunks: Vec<Option<Vec<RecordReadOutcome>>> = vec![None; raw_chunks.len()];
        let mut in_flight = FuturesUnordered::new();

        for (chunk_idx, chunk_keys) in raw_chunks.into_iter().enumerate() {
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

    async fn read_keys_subbatch(
        &self,
        chunk_idx: usize,
        keys: Vec<Key>,
        bins: Option<Vec<String>>,
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

        let res = self.execute_key_reads_with_retry(keys, bins.as_deref()).await;

        let elapsed_micros = start.elapsed().as_micros() as u64;
        self.metrics.observe_read_latency_micros(elapsed_micros);
        self.metrics.add_in_flight(-(num_records as i64));

        res.map(|outcomes| (chunk_idx, outcomes))
    }

    async fn execute_key_reads_with_retry(
        &self,
        initial_keys: Vec<Key>,
        bins: Option<&[String]>,
    ) -> Result<Vec<RecordReadOutcome>> {
        let total = initial_keys.len();
        let mut results: Vec<Option<RecordReadOutcome>> = vec![None; total];
        let mut pending_keys: Vec<Key> = initial_keys;
        let mut pending_meta: Vec<(usize, u32, u32)> =
            (0..total).map(|orig_idx| (orig_idx, 0, 0)).collect();

        let mut backoff_step: u32 = 0;

        while !pending_keys.is_empty() {
            let outcomes = self
                .sink
                .execute_batch_read_keys(&pending_keys, bins)
                .await;

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
