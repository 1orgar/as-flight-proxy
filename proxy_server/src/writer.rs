use aerospike::operations;
use aerospike::{
    BatchOperation, BatchPolicy, BatchWritePolicy, Client, ClientResultCode, Concurrency,
    ErrorKind, ResultCode, WritePolicy,
};
use futures::stream::{FuturesUnordered, StreamExt};
use rand::Rng;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tracing::{debug, warn};

use crate::config::{RetryConfig, WriteParameters, WriterConfig};
use crate::converter::PreparedRecord;
use crate::error::{ProxyError, Result};
use crate::metrics::ProxyMetrics;

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

/// Statistics returned after writing an Arrow `RecordBatch`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BatchWriteStats {
    pub rows_written: usize,
    pub overload_retries: u64,
    pub other_retries: u64,
}

/// Abstraction over the Aerospike client for executing batch and single-key writes.
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
                for op in &batch_ops {
                    match op.result_code() {
                        Some(ResultCode::Ok) => outcomes.push(RecordWriteOutcome::Success),
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
                for op in &batch_ops {
                    if op.result_code() == Some(ResultCode::Ok) {
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

        match self.client.put(&wp, &record.key, &record.bins).await {
            Ok(()) => RecordWriteOutcome::Success,
            Err(err) => classify_aerospike_failure(Some(&err), None),
        }
    }

    fn is_connected(&self) -> bool {
        self.client.is_connected()
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

    /// Write a vector of `PreparedRecord`s concurrently in MPP sub-batches.
    pub async fn write_records(
        &self,
        records: Vec<PreparedRecord>,
        params: &WriteParameters,
    ) -> Result<BatchWriteStats> {
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

        let chunks: Vec<Vec<PreparedRecord>> = records
            .chunks(chunk_size)
            .map(|c| c.to_vec())
            .collect();

        let mut in_flight_futures = FuturesUnordered::new();
        let mut stats = BatchWriteStats {
            rows_written: total_rows,
            overload_retries: 0,
            other_retries: 0,
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
        #[derive(Clone)]
        struct PendingItem {
            record: PreparedRecord,
            overload_attempts: u32,
            other_attempts: u32,
        }

        let mut pending: Vec<PendingItem> = initial_records
            .into_iter()
            .map(|record| PendingItem {
                record,
                overload_attempts: 0,
                other_attempts: 0,
            })
            .collect();

        let mut total_overload_retries: u64 = 0;
        let mut total_other_retries: u64 = 0;
        let mut backoff_step: u32 = 0;

        while !pending.is_empty() {
            let batch_slice: Vec<PreparedRecord> =
                pending.iter().map(|p| p.record.clone()).collect();
            let outcomes = self.sink.execute_batch_write(&batch_slice, params).await;

            let mut next_pending = Vec::new();
            let mut batch_overload_count: u64 = 0;
            let mut batch_other_count: u64 = 0;

            for (mut item, outcome) in pending.into_iter().zip(outcomes.into_iter()) {
                match outcome {
                    RecordWriteOutcome::Success => {
                        // Successfully written; do not re-queue.
                    }
                    RecordWriteOutcome::OverloadError(err_msg) => {
                        item.overload_attempts = item.overload_attempts.saturating_add(1);
                        batch_overload_count += 1;
                        debug!(
                            key = ?item.record.key,
                            overload_attempt = item.overload_attempts,
                            error = %err_msg,
                            "Overload error on key; scheduling infinite backoff retry"
                        );
                        next_pending.push(item);
                    }
                    RecordWriteOutcome::OtherError(err_msg) => {
                        item.other_attempts = item.other_attempts.saturating_add(1);
                        if item.other_attempts > self.retry_config.max_other_retries {
                            self.metrics.inc_failed_records(1);
                            warn!(
                                key = ?item.record.key,
                                attempts = item.other_attempts,
                                error = %err_msg,
                                "Non-overload error exceeded max_other_retries; terminating stream"
                            );
                            return Err(ProxyError::WriteFailed {
                                attempts: item.other_attempts,
                                message: err_msg,
                            });
                        }
                        batch_other_count += 1;
                        warn!(
                            key = ?item.record.key,
                            other_attempt = item.other_attempts,
                            max_other_retries = self.retry_config.max_other_retries,
                            error = %err_msg,
                            "Non-overload error on key; retrying"
                        );
                        next_pending.push(item);
                    }
                }
            }

            if next_pending.is_empty() {
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

            pending = next_pending;
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
