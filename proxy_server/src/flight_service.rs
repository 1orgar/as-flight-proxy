use arrow_array::RecordBatch;
use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::{Field, Schema};
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt, TryStreamExt};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::codec::CompressionEncoding;
use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info};

use crate::auth::Authenticator;
use crate::config::{
    FlightCompression, InferSchemaRequest, LookupExchangeParams, LookupJoinType, ReadModeSpec,
    ReadTicket, ServerConfig, WriteParameters,
};
use crate::converter::{
    ScannedRecord, build_lookup_record_batch, decode_arrow_schema_ipc_base64,
    encode_arrow_schema_to_result, extract_keys_from_batch, infer_arrow_schema_from_records,
    records_to_record_batch,
};
use crate::error::ProxyError;
use crate::metrics::ProxyMetrics;
use crate::raft::{JobState, MetadataCommand, RaftNode, now_ms};
use crate::writer::{MppWriteEngine, RecordReadOutcome};

/// JSON payload returned in `PutResult.app_metadata` after each `RecordBatch` is committed or validated.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PutBatchAck {
    pub batch_index: usize,
    pub rows_written: usize,
    pub overload_retries: u64,
    pub other_retries: u64,
    #[serde(default)]
    pub node_id: u64,
    #[serde(default)]
    pub rows_validated: usize,
    #[serde(default)]
    pub matched_rows: usize,
    #[serde(default)]
    pub mismatched_rows: usize,
    #[serde(default)]
    pub missing_keys: usize,
    #[serde(default)]
    pub bin_mismatch_rows: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct JobStateActionPayload {
    pub job_id: String,
    #[serde(default)]
    pub namespace: String,
    #[serde(default)]
    pub set_name: String,
    #[serde(default)]
    pub is_dq_validation: bool,
    #[serde(default)]
    pub error: Option<String>,
}

struct ActiveStreamGuard {
    metrics: Arc<ProxyMetrics>,
}

impl ActiveStreamGuard {
    fn new(metrics: Arc<ProxyMetrics>) -> Self {
        metrics.add_active_stream(1);
        Self { metrics }
    }
}

impl Drop for ActiveStreamGuard {
    fn drop(&mut self) {
        self.metrics.add_active_stream(-1);
    }
}

#[derive(Clone)]
pub struct ProxyFlightService {
    authenticator: Arc<Authenticator>,
    write_engine: Arc<MppWriteEngine>,
    metrics: Arc<ProxyMetrics>,
    max_bin_name_len: usize,
    raft_node: Option<Arc<RaftNode>>,
    compression_enabled: bool,
    default_compression: FlightCompression,
    default_compression_level: i32,
}

impl ProxyFlightService {
    pub fn new(
        authenticator: Arc<Authenticator>,
        write_engine: Arc<MppWriteEngine>,
        metrics: Arc<ProxyMetrics>,
        max_bin_name_len: usize,
    ) -> Self {
        Self {
            authenticator,
            write_engine,
            metrics,
            max_bin_name_len,
            raft_node: None,
            compression_enabled: true,
            default_compression: FlightCompression::default(),
            default_compression_level: FlightCompression::DEFAULT_ZSTD_LEVEL,
        }
    }

    pub fn with_raft_node(mut self, raft_node: Arc<RaftNode>) -> Self {
        self.raft_node = Some(raft_node);
        self
    }

    pub fn with_compression(
        mut self,
        enabled: bool,
        default_compression: FlightCompression,
    ) -> Self {
        self.compression_enabled = enabled;
        self.default_compression = if enabled {
            default_compression
        } else {
            FlightCompression::None
        };
        self
    }

    pub fn with_compression_level(mut self, level: i32) -> Self {
        self.default_compression_level = level;
        self
    }

    /// Wrap this service in a `FlightServiceServer` configured with message size limits
    /// and gRPC transport compression settings from `ServerConfig`.
    pub fn into_server(mut self, server_cfg: &ServerConfig) -> FlightServiceServer<Self> {
        self.default_compression_level = server_cfg.compression_level;
        let max_msg_bytes = server_cfg.max_decoding_message_size_mb * 1024 * 1024;
        let mut svc = FlightServiceServer::new(self)
            .max_decoding_message_size(max_msg_bytes)
            .max_encoding_message_size(max_msg_bytes);

        if server_cfg.compression_enabled {
            svc = svc
                .accept_compressed(CompressionEncoding::Gzip)
                .accept_compressed(CompressionEncoding::Zstd);
            if server_cfg.compression == FlightCompression::Gzip {
                svc = svc.send_compressed(CompressionEncoding::Gzip);
            }
        }
        svc
    }

    fn resolve_effective_compression(
        &self,
        client_override: Option<FlightCompression>,
        header_override: Option<FlightCompression>,
    ) -> FlightCompression {
        if !self.compression_enabled {
            return FlightCompression::None;
        }
        client_override
            .or(header_override)
            .unwrap_or(self.default_compression)
    }

    fn resolve_ipc_write_options(
        &self,
        client_override: Option<FlightCompression>,
        client_level_override: Option<i32>,
        headers: &MetadataMap,
    ) -> Result<IpcWriteOptions, Status> {
        let header_override = FlightCompression::from_headers(headers).map_err(Status::from)?;
        let header_level = FlightCompression::level_from_headers(headers).map_err(Status::from)?;
        let effective = self.resolve_effective_compression(client_override, header_override);
        let effective_level = client_level_override
            .or(header_level)
            .or(Some(self.default_compression_level));
        effective
            .to_ipc_write_options_with_level(effective_level)
            .map_err(Status::from)
    }
}

#[tonic::async_trait]
impl FlightService for ProxyFlightService {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let metadata = request.metadata().clone();
        let mut inbound = request.into_inner();

        let has_auth_header =
            metadata.contains_key("authorization") || metadata.contains_key("Authorization");

        let (protocol_version, token) = if has_auth_header {
            let token = self
                .authenticator
                .authenticate_handshake(&metadata, &[])
                .map_err(Status::from)?;
            (0, token)
        } else {
            let first_msg = inbound.message().await?;
            let proto_ver = first_msg.as_ref().map(|m| m.protocol_version).unwrap_or(0);
            let payload = first_msg
                .as_ref()
                .map(|m| m.payload.as_ref())
                .unwrap_or(&[]);
            let token = self
                .authenticator
                .authenticate_handshake(&metadata, payload)
                .map_err(Status::from)?;
            (proto_ver, token)
        };

        let bearer_header = format!("Bearer {token}");
        let header_val = MetadataValue::try_from(bearer_header.as_str())
            .map_err(|e| Status::internal(format!("Failed to encode Bearer header: {e}")))?;

        let response_msg = HandshakeResponse {
            protocol_version,
            payload: Bytes::from(token.into_bytes()),
        };

        let output_stream = stream::once(async move { Ok(response_msg) }).boxed();
        let mut response = Response::new(output_stream);
        response.metadata_mut().insert("authorization", header_val);

        Ok(response)
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        // 1. Verify authentication before reading stream data
        self.authenticator
            .verify_metadata(request.metadata())
            .map_err(Status::from)?;

        let headers = request.metadata().clone();
        let flight_data_stream = request.into_inner().map_err(FlightError::from);
        let mut decoder = FlightDataDecoder::new(flight_data_stream);

        let write_engine = Arc::clone(&self.write_engine);
        let metrics = Arc::clone(&self.metrics);
        let max_bin_name_len = self.max_bin_name_len;
        let raft_node = self.raft_node.clone();
        let node_id = raft_node.as_ref().map(|r| r.node_id()).unwrap_or(1);

        let (tx, rx) = mpsc::channel::<Result<PutResult, Status>>(32);

        tokio::spawn(async move {
            let _stream_guard = ActiveStreamGuard::new(Arc::clone(&metrics));
            let mut write_params: Option<WriteParameters> = None;
            let mut batch_index: usize = 0;
            let mut total_stream_rows: usize = 0;

            while let Some(decoded_res) = decoder.next().await {
                let decoded = match decoded_res {
                    Ok(d) => d,
                    Err(e) => {
                        error!("FlightData decode error in do_put stream: {e}");
                        let _ = tx.send(Err(ProxyError::Flight(e).into())).await;
                        return;
                    }
                };

                // Extract WriteParameters from the first FlightDescriptor we see
                if write_params.is_none() {
                    if let Some(ref desc) = decoded.inner.flight_descriptor {
                        match WriteParameters::from_descriptor_or_headers(
                            Some(desc.cmd.as_ref()),
                            &headers,
                        ) {
                            Ok(params) => {
                                debug!(
                                    job_id = ?params.job_id,
                                    namespace = %params.namespace,
                                    set_name = %params.set_name,
                                    key_column = %params.key_column,
                                    write_mode = ?params.write_mode,
                                    ignore_nulls = params.ignore_nulls,
                                    "Initialized DoPut stream WriteParameters from FlightDescriptor"
                                );
                                write_params = Some(params);
                            }
                            Err(err) => {
                                let _ = tx.send(Err(err.into())).await;
                                return;
                            }
                        }
                    }
                }

                match decoded.payload {
                    DecodedPayload::None | DecodedPayload::Schema(_) => {
                        // Schema or metadata message; continue to next message.
                    }
                    DecodedPayload::RecordBatch(batch) => {
                        if write_params.is_none() {
                            match WriteParameters::from_descriptor_or_headers(None, &headers) {
                                Ok(params) => write_params = Some(params),
                                Err(err) => {
                                    let _ = tx.send(Err(err.into())).await;
                                    return;
                                }
                            }
                        }

                        let params = write_params.as_ref().expect("write_params initialized");
                        let write_res = write_engine
                            .write_record_batch(&batch, params, max_bin_name_len)
                            .await;
                        // Drop the Arrow RecordBatch immediately after Aerospike execution so its buffers
                        // are not held in memory while proposing batch stats to Raft.
                        drop(batch);

                        let stats = match write_res {
                            Ok(s) => s,
                            Err(err) => {
                                error!("MPP write engine error on batch {batch_index}: {err}");
                                if let (Some(raft), Some(job_id)) = (&raft_node, &params.job_id) {
                                    let _ = raft
                                        .propose(MetadataCommand::SetJobState {
                                            job_id: job_id.clone(),
                                            namespace: params.namespace.clone(),
                                            set_name: params.set_name.clone(),
                                            state: JobState::Failed,
                                            error_message: Some(err.to_string()),
                                            timestamp_ms: now_ms(),
                                        })
                                        .await;
                                }
                                let _ = tx.send(Err(err.into())).await;
                                return;
                            }
                        };

                        // Replicate job batch progress across the proxy cluster via Raft
                        if let (Some(raft), Some(job_id)) = (&raft_node, &params.job_id) {
                            let _ = raft
                                .propose(MetadataCommand::RecordJobBatch {
                                    job_id: job_id.clone(),
                                    node_id,
                                    namespace: params.namespace.clone(),
                                    set_name: params.set_name.clone(),
                                    is_dq_validation: params.is_validate_mode(),
                                    rows_written: stats.rows_written as u64,
                                    overload_retries: stats.overload_retries,
                                    other_retries: stats.other_retries,
                                    rows_validated: stats.rows_validated as u64,
                                    matched_rows: stats.matched_rows as u64,
                                    mismatched_rows: stats.mismatched_rows as u64,
                                    missing_keys: stats.missing_keys as u64,
                                    bin_mismatch_rows: stats.bin_mismatch_rows as u64,
                                    mismatch_samples: stats.mismatch_samples,
                                    timestamp_ms: now_ms(),
                                })
                                .await;
                        }

                        total_stream_rows += stats.rows_written + stats.rows_validated;
                        let ack = PutBatchAck {
                            batch_index,
                            rows_written: stats.rows_written,
                            overload_retries: stats.overload_retries,
                            other_retries: stats.other_retries,
                            node_id,
                            rows_validated: stats.rows_validated,
                            matched_rows: stats.matched_rows,
                            mismatched_rows: stats.mismatched_rows,
                            missing_keys: stats.missing_keys,
                            bin_mismatch_rows: stats.bin_mismatch_rows,
                        };
                        batch_index += 1;

                        let ack_bytes = serde_json::to_vec(&ack).unwrap_or_default();
                        if tx
                            .send(Ok(PutResult {
                                app_metadata: Bytes::from(ack_bytes),
                            }))
                            .await
                            .is_err()
                        {
                            // Client closed stream early
                            return;
                        }
                    }
                }
            }

            info!(
                node_id,
                batches = batch_index,
                total_rows = total_stream_rows,
                "Completed Arrow Flight DoPut stream"
            );
        });

        let output_stream = ReceiverStream::new(rx).boxed();
        Ok(Response::new(output_stream))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        self.authenticator
            .verify_metadata(request.metadata())
            .map_err(Status::from)?;

        let action = request.into_inner();
        let Some(ref raft) = self.raft_node else {
            return Err(Status::unavailable(
                "Raft metadata service is not enabled on this proxy node",
            ));
        };

        let result_bytes: Vec<u8> = match action.r#type.as_str() {
            "get_job_status" => {
                let job_id = parse_job_id_from_body(&action.body)?;
                match raft.get_job_status(&job_id).await {
                    Some(status) => serde_json::to_vec(&status)
                        .map_err(|e| Status::internal(format!("JSON serialize error: {e}")))?,
                    None => {
                        return Err(Status::not_found(format!(
                            "Job '{job_id}' not found in Raft cluster state"
                        )));
                    }
                }
            }

            "get_cluster_status" => {
                let snapshot = raft.get_cluster_snapshot().await;
                serde_json::to_vec(&snapshot)
                    .map_err(|e| Status::internal(format!("JSON serialize error: {e}")))?
            }

            "init_job" | "reset_job" => {
                let payload = parse_job_state_payload(&action.body)?;
                raft.propose(MetadataCommand::ResetJob {
                    job_id: payload.job_id.clone(),
                    namespace: payload.namespace,
                    set_name: payload.set_name,
                    is_dq_validation: payload.is_dq_validation,
                    timestamp_ms: now_ms(),
                })
                .await
                .map_err(Status::from)?;

                let updated = raft.get_job_status(&payload.job_id).await;
                serde_json::to_vec(&updated)
                    .map_err(|e| Status::internal(format!("JSON serialize error: {e}")))?
            }

            "complete_job" => {
                let payload = parse_job_state_payload(&action.body)?;
                raft.propose(MetadataCommand::SetJobState {
                    job_id: payload.job_id.clone(),
                    namespace: payload.namespace,
                    set_name: payload.set_name,
                    state: JobState::Completed,
                    error_message: None,
                    timestamp_ms: now_ms(),
                })
                .await
                .map_err(Status::from)?;

                let updated = raft.get_job_status(&payload.job_id).await;
                serde_json::to_vec(&updated)
                    .map_err(|e| Status::internal(format!("JSON serialize error: {e}")))?
            }

            "fail_job" => {
                let payload = parse_job_state_payload(&action.body)?;
                raft.propose(MetadataCommand::SetJobState {
                    job_id: payload.job_id.clone(),
                    namespace: payload.namespace,
                    set_name: payload.set_name,
                    state: JobState::Failed,
                    error_message: payload.error,
                    timestamp_ms: now_ms(),
                })
                .await
                .map_err(Status::from)?;

                let updated = raft.get_job_status(&payload.job_id).await;
                serde_json::to_vec(&updated)
                    .map_err(|e| Status::internal(format!("JSON serialize error: {e}")))?
            }

            other => {
                return Err(Status::invalid_argument(format!(
                    "Unsupported action type '{other}'. Supported: init_job, reset_job, get_job_status, get_cluster_status, complete_job, fail_job"
                )));
            }
        };

        let res = arrow_flight::Result {
            body: Bytes::from(result_bytes),
        };
        let stream = stream::once(async move { Ok(res) }).boxed();
        Ok(Response::new(stream))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![
            Ok(ActionType {
                r#type: "init_job".to_string(),
                description: "Initialize or reset Raft-replicated metadata for a Spark job_id".to_string(),
            }),
            Ok(ActionType {
                r#type: "get_job_status".to_string(),
                description: "Get Raft-replicated load status for a Spark job_id".to_string(),
            }),
            Ok(ActionType {
                r#type: "get_cluster_status".to_string(),
                description: "Get Raft cluster topology, node metrics, and all jobs".to_string(),
            }),
            Ok(ActionType {
                r#type: "complete_job".to_string(),
                description: "Mark a Spark load job_id as completed in Raft".to_string(),
            }),
            Ok(ActionType {
                r#type: "fail_job".to_string(),
                description: "Mark a Spark load job_id as failed in Raft".to_string(),
            }),
        ];
        Ok(Response::new(stream::iter(actions).boxed()))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights is not supported"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info is not supported"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info is not supported"))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        self.authenticator
            .verify_metadata(request.metadata())
            .map_err(Status::from)?;

        let descriptor = request.into_inner();
        let infer_req: InferSchemaRequest =
            serde_json::from_slice(&descriptor.cmd).map_err(|e| {
                Status::invalid_argument(format!(
                    "Failed to parse InferSchemaRequest JSON from FlightDescriptor.cmd: {e}"
                ))
            })?;
        infer_req.validate().map_err(Status::from)?;

        let start = std::time::Instant::now();
        let sample_size = infer_req.sample_size.max(1);
        let mut rx = self
            .write_engine
            .scan_partition_range(
                &infer_req.namespace,
                &infer_req.set_name,
                0,
                4096,
                None,
                Some(sample_size as u64),
            )
            .await
            .map_err(Status::from)?;

        let mut sampled_records = Vec::with_capacity(sample_size.min(4096));
        while let Some(res) = rx.recv().await {
            let rec = res.map_err(Status::from)?;
            sampled_records.push(rec);
            if sampled_records.len() >= sample_size {
                break;
            }
        }

        let elapsed_micros = start.elapsed().as_micros() as u64;
        self.metrics.observe_read_latency_micros(elapsed_micros);

        let schema = infer_arrow_schema_from_records(&infer_req.key_column, &sampled_records);
        let node_id = self.raft_node.as_ref().map(|r| r.node_id()).unwrap_or(1);
        info!(
            node_id,
            namespace = %infer_req.namespace,
            set_name = %infer_req.set_name,
            sampled = sampled_records.len(),
            inferred_fields = schema.fields().len(),
            elapsed_ms = elapsed_micros / 1000,
            "Completed Arrow Flight GetSchema inference"
        );
        let schema_result = encode_arrow_schema_to_result(&schema).map_err(Status::from)?;
        Ok(Response::new(schema_result))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        self.authenticator
            .verify_metadata(request.metadata())
            .map_err(Status::from)?;

        let headers = request.metadata().clone();
        let ticket = request.into_inner();
        let read_ticket: ReadTicket = serde_json::from_slice(&ticket.ticket).map_err(|e| {
            Status::invalid_argument(format!("Failed to parse ReadTicket JSON from Ticket: {e}"))
        })?;
        read_ticket.validate().map_err(Status::from)?;

        let ipc_write_options = self.resolve_ipc_write_options(
            read_ticket.compression,
            read_ticket.compression_level,
            &headers,
        )?;

        let target_schema = decode_arrow_schema_ipc_base64(&read_ticket.schema_ipc_base64)
            .map_err(Status::from)?;

        let requested_bins: Vec<String> = if !read_ticket.bins.is_empty() {
            read_ticket.bins.clone()
        } else {
            target_schema
                .fields()
                .iter()
                .filter(|f| f.name() != &read_ticket.key_column)
                .map(|f| f.name().clone())
                .collect()
        };

        let write_engine = Arc::clone(&self.write_engine);
        let metrics = Arc::clone(&self.metrics);
        let schema_for_task = Arc::clone(&target_schema);
        let batch_size = read_ticket.batch_size.max(1);
        let node_id = self.raft_node.as_ref().map(|r| r.node_id()).unwrap_or(1);

        let (batch_tx, batch_rx) =
            mpsc::channel::<std::result::Result<RecordBatch, FlightError>>(8);

        tokio::spawn(async move {
            let _stream_guard = ActiveStreamGuard::new(Arc::clone(&metrics));
            let stream_start = std::time::Instant::now();
            let mut total_rows_emitted: usize = 0;
            let mut total_batches_emitted: usize = 0;

            match read_ticket.read_mode {
                ReadModeSpec::PartitionRange {
                    begin_partition,
                    partition_count,
                } => {
                    let scan_start = std::time::Instant::now();
                    let mut rx = match write_engine
                        .scan_partition_range(
                            &read_ticket.namespace,
                            &read_ticket.set_name,
                            begin_partition,
                            partition_count,
                            Some(requested_bins),
                            None,
                        )
                        .await
                    {
                        Ok(r) => r,
                        Err(e) => {
                            let _ = batch_tx
                                .send(Err(FlightError::Tonic(Box::new(Status::from(e)))))
                                .await;
                            return;
                        }
                    };

                    let mut buffer: Vec<ScannedRecord> = Vec::with_capacity(batch_size);
                    while let Some(res) = rx.recv().await {
                        match res {
                            Ok(rec) => {
                                buffer.push(rec);
                                if buffer.len() >= batch_size {
                                    match records_to_record_batch(
                                        &schema_for_task,
                                        &read_ticket.key_column,
                                        &buffer,
                                        read_ticket.type_mismatch_mode,
                                    ) {
                                        Ok(rb) => {
                                            let rows = rb.num_rows();
                                            total_rows_emitted += rows;
                                            total_batches_emitted += 1;
                                            metrics.inc_records_read(rows as u64);
                                            metrics.inc_read_batches();
                                            buffer.clear();
                                            if batch_tx.send(Ok(rb)).await.is_err() {
                                                return;
                                            }
                                        }
                                        Err(e) => {
                                            let _ = batch_tx
                                                .send(Err(FlightError::Tonic(Box::new(
                                                    Status::from(e),
                                                ))))
                                                .await;
                                            return;
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                let _ = batch_tx
                                    .send(Err(FlightError::Tonic(Box::new(Status::from(e)))))
                                    .await;
                                return;
                            }
                        }
                    }

                    if !buffer.is_empty() {
                        match records_to_record_batch(
                            &schema_for_task,
                            &read_ticket.key_column,
                            &buffer,
                            read_ticket.type_mismatch_mode,
                        ) {
                            Ok(rb) => {
                                let rows = rb.num_rows();
                                total_rows_emitted += rows;
                                total_batches_emitted += 1;
                                metrics.inc_records_read(rows as u64);
                                metrics.inc_read_batches();
                                let _ = batch_tx.send(Ok(rb)).await;
                            }
                            Err(e) => {
                                let _ = batch_tx
                                    .send(Err(FlightError::Tonic(Box::new(Status::from(e)))))
                                    .await;
                            }
                        }
                    }

                    metrics.observe_read_latency_micros(scan_start.elapsed().as_micros() as u64);
                    info!(
                        node_id,
                        namespace = %read_ticket.namespace,
                        set_name = %read_ticket.set_name,
                        begin_partition,
                        partition_count,
                        batches = total_batches_emitted,
                        rows = total_rows_emitted,
                        elapsed_ms = stream_start.elapsed().as_millis() as u64,
                        "Completed Arrow Flight DoGet PartitionRange stream"
                    );
                }

                ReadModeSpec::BatchKeys { keys } => {
                    let total_requested_keys = keys.len();
                    let mut as_keys = Vec::with_capacity(total_requested_keys);
                    for lit in &keys {
                        match lit.to_key(&read_ticket.namespace, &read_ticket.set_name) {
                            Ok(k) => as_keys.push(k),
                            Err(e) => {
                                let _ = batch_tx
                                    .send(Err(FlightError::Tonic(Box::new(Status::from(e)))))
                                    .await;
                                return;
                            }
                        }
                    }

                    let mut key_iter = as_keys.into_iter();
                    loop {
                        let chunk: Vec<aerospike::Key> =
                            key_iter.by_ref().take(batch_size).collect();
                        if chunk.is_empty() {
                            break;
                        }
                        let chunk_len = chunk.len();
                        let outcomes = match write_engine
                            .read_keys(chunk.clone(), Some(requested_bins.clone()))
                            .await
                        {
                            Ok(o) => o,
                            Err(e) => {
                                let _ = batch_tx
                                    .send(Err(FlightError::Tonic(Box::new(Status::from(e)))))
                                    .await;
                                return;
                            }
                        };

                        let mut found_records = Vec::with_capacity(chunk_len);
                        for (key, outcome) in chunk.into_iter().zip(outcomes.into_iter()) {
                            if let RecordReadOutcome::Found(map) = outcome {
                                let mut bins = IndexMap::with_capacity(map.len());
                                for (k, v) in map {
                                    bins.insert(k, v);
                                }
                                found_records.push(ScannedRecord {
                                    key: Some(key),
                                    bins,
                                });
                            }
                        }

                        if !found_records.is_empty() {
                            match records_to_record_batch(
                                &schema_for_task,
                                &read_ticket.key_column,
                                &found_records,
                                read_ticket.type_mismatch_mode,
                            ) {
                                Ok(rb) => {
                                    let rows = rb.num_rows();
                                    total_rows_emitted += rows;
                                    total_batches_emitted += 1;
                                    metrics.inc_records_read(rows as u64);
                                    metrics.inc_read_batches();
                                    if batch_tx.send(Ok(rb)).await.is_err() {
                                        return;
                                    }
                                }
                                Err(e) => {
                                    let _ = batch_tx
                                        .send(Err(FlightError::Tonic(Box::new(Status::from(e)))))
                                        .await;
                                    return;
                                }
                            }
                        }
                    }

                    info!(
                        node_id,
                        namespace = %read_ticket.namespace,
                        set_name = %read_ticket.set_name,
                        requested_keys = total_requested_keys,
                        batches = total_batches_emitted,
                        rows = total_rows_emitted,
                        elapsed_ms = stream_start.elapsed().as_millis() as u64,
                        "Completed Arrow Flight DoGet BatchKeys stream"
                    );
                }
            }
        });

        let encoded_stream = FlightDataEncoderBuilder::new()
            .with_schema(target_schema)
            .with_options(ipc_write_options)
            .build(ReceiverStream::new(batch_rx))
            .map_err(Status::from)
            .boxed();

        Ok(Response::new(encoded_stream))
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        self.authenticator
            .verify_metadata(request.metadata())
            .map_err(Status::from)?;

        let header_compression =
            FlightCompression::from_headers(request.metadata()).map_err(Status::from)?;
        let header_compression_level =
            FlightCompression::level_from_headers(request.metadata()).map_err(Status::from)?;
        let compression_enabled = self.compression_enabled;
        let default_compression = self.default_compression;
        let default_compression_level = self.default_compression_level;

        let flight_data_stream = request.into_inner().map_err(FlightError::from);
        let mut decoder = FlightDataDecoder::new(flight_data_stream);

        let write_engine = Arc::clone(&self.write_engine);
        let metrics = Arc::clone(&self.metrics);
        let node_id = self.raft_node.as_ref().map(|r| r.node_id()).unwrap_or(1);

        let (flight_tx, flight_rx) = mpsc::channel::<Result<FlightData, Status>>(16);

        tokio::spawn(async move {
            let _stream_guard = ActiveStreamGuard::new(Arc::clone(&metrics));
            let stream_start = std::time::Instant::now();
            let mut lookup_params: Option<LookupExchangeParams> = None;
            let mut bins_schema: Option<Arc<Schema>> = None;
            let mut requested_bins: Vec<String> = Vec::new();
            let mut out_schema_opt: Option<Arc<Schema>> = None;
            let mut sent_any_batch = false;
            let mut total_keys_req: usize = 0;
            let mut total_keys_found: usize = 0;
            let mut total_rows_out: usize = 0;
            let mut total_batches_out: usize = 0;

            let mut batch_tx_opt: Option<mpsc::Sender<std::result::Result<RecordBatch, FlightError>>> =
                None;
            let mut encoder_handle: Option<tokio::task::JoinHandle<()>> = None;

            let init_encoder = |params_opt: &Option<LookupExchangeParams>,
                                ftx: mpsc::Sender<Result<FlightData, Status>>|
             -> (
                mpsc::Sender<std::result::Result<RecordBatch, FlightError>>,
                tokio::task::JoinHandle<()>,
            ) {
                let effective_comp = if !compression_enabled {
                    FlightCompression::None
                } else {
                    params_opt
                        .as_ref()
                        .and_then(|p| p.compression)
                        .or(header_compression)
                        .unwrap_or(default_compression)
                };
                let effective_level = params_opt
                    .as_ref()
                    .and_then(|p| p.compression_level)
                    .or(header_compression_level)
                    .or(Some(default_compression_level));
                let ipc_opts = effective_comp
                    .to_ipc_write_options_with_level(effective_level)
                    .unwrap_or_default();
                let (btx, brx) =
                    mpsc::channel::<std::result::Result<RecordBatch, FlightError>>(8);
                let handle = tokio::spawn(async move {
                    let mut enc_stream = FlightDataEncoderBuilder::new()
                        .with_options(ipc_opts)
                        .build(ReceiverStream::new(brx));
                    while let Some(item) = enc_stream.next().await {
                        if ftx.send(item.map_err(Status::from)).await.is_err() {
                            break;
                        }
                    }
                });
                (btx, handle)
            };

            while let Some(decoded_res) = decoder.next().await {
                let decoded = match decoded_res {
                    Ok(d) => d,
                    Err(e) => {
                        drop(batch_tx_opt);
                        if let Some(h) = encoder_handle {
                            let _ = h.await;
                        }
                        let _ = flight_tx.send(Err(Status::from(e))).await;
                        return;
                    }
                };

                if lookup_params.is_none() {
                    if let Some(ref desc) = decoded.inner.flight_descriptor {
                        match serde_json::from_slice::<LookupExchangeParams>(&desc.cmd) {
                            Ok(p) => {
                                if let Err(e) = p.validate() {
                                    let _ = flight_tx.send(Err(Status::from(e))).await;
                                    return;
                                }
                                match decode_arrow_schema_ipc_base64(&p.bins_schema_ipc_base64) {
                                    Ok(bs) => {
                                        requested_bins = bs
                                            .fields()
                                            .iter()
                                            .filter(|f| f.name() != &p.key_column)
                                            .map(|f| f.name().clone())
                                            .collect();
                                        bins_schema = Some(bs);
                                        lookup_params = Some(p);
                                    }
                                    Err(e) => {
                                        let _ = flight_tx.send(Err(Status::from(e))).await;
                                        return;
                                    }
                                }
                            }
                            Err(e) => {
                                let _ = flight_tx
                                    .send(Err(Status::invalid_argument(format!(
                                        "Invalid LookupExchangeParams JSON: {e}"
                                    ))))
                                    .await;
                                return;
                            }
                        }
                    }
                }

                match decoded.payload {
                    DecodedPayload::None => {}
                    DecodedPayload::Schema(input_schema) => {
                        if let (Some(p), Some(bs)) = (&lookup_params, &bins_schema) {
                            let mut fields: Vec<Arc<Field>> =
                                input_schema.fields().iter().cloned().collect();
                            for f in bs.fields() {
                                if f.name() == &p.key_column
                                    && input_schema.index_of(&p.key_column).is_ok()
                                {
                                    continue;
                                }
                                let nullable =
                                    f.is_nullable() || p.join_type == LookupJoinType::Left;
                                fields.push(Arc::new(f.as_ref().clone().with_nullable(nullable)));
                            }
                            out_schema_opt = Some(Arc::new(Schema::new(fields)));
                        }
                    }
                    DecodedPayload::RecordBatch(input_batch) => {
                        let (Some(p), Some(bs)) = (&lookup_params, &bins_schema) else {
                            drop(batch_tx_opt);
                            if let Some(h) = encoder_handle {
                                let _ = h.await;
                            }
                            let _ = flight_tx
                                .send(Err(Status::invalid_argument(
                                    "Missing LookupExchangeParams FlightDescriptor before RecordBatch",
                                )))
                                .await;
                            return;
                        };

                        let keys = match extract_keys_from_batch(
                            &input_batch,
                            &p.namespace,
                            &p.set_name,
                            &p.key_column,
                        ) {
                            Ok(k) => k,
                            Err(e) => {
                                drop(batch_tx_opt);
                                if let Some(h) = encoder_handle {
                                    let _ = h.await;
                                }
                                let _ = flight_tx.send(Err(Status::from(e))).await;
                                return;
                            }
                        };

                        let num_keys = keys.len();
                        total_keys_req += num_keys;
                        metrics.inc_lookup_keys_requested(num_keys as u64);

                        let outcomes = match write_engine
                            .read_keys(keys, Some(requested_bins.clone()))
                            .await
                        {
                            Ok(o) => o,
                            Err(e) => {
                                drop(batch_tx_opt);
                                if let Some(h) = encoder_handle {
                                    let _ = h.await;
                                }
                                let _ = flight_tx.send(Err(Status::from(e))).await;
                                return;
                            }
                        };

                        let found_in_batch = outcomes
                            .iter()
                            .filter(|o| matches!(o, RecordReadOutcome::Found(_)))
                            .count();
                        total_keys_found += found_in_batch;
                        metrics.inc_lookup_keys_found(found_in_batch as u64);

                        match build_lookup_record_batch(
                            &input_batch,
                            bs,
                            &p.key_column,
                            &outcomes,
                            p.join_type,
                            p.type_mismatch_mode,
                        ) {
                            Ok(Some(out_batch)) => {
                                let rows = out_batch.num_rows();
                                total_rows_out += rows;
                                total_batches_out += 1;
                                metrics.inc_records_read(rows as u64);
                                metrics.inc_read_batches();
                                sent_any_batch = true;
                                if batch_tx_opt.is_none() {
                                    let (btx, handle) =
                                        init_encoder(&lookup_params, flight_tx.clone());
                                    batch_tx_opt = Some(btx);
                                    encoder_handle = Some(handle);
                                }
                                if batch_tx_opt
                                    .as_ref()
                                    .unwrap()
                                    .send(Ok(out_batch))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                            Ok(None) => {
                                // All rows filtered out in Inner join mode for this batch
                            }
                            Err(e) => {
                                drop(batch_tx_opt);
                                if let Some(h) = encoder_handle {
                                    let _ = h.await;
                                }
                                let _ = flight_tx.send(Err(Status::from(e))).await;
                                return;
                            }
                        }
                    }
                }
            }

            if !sent_any_batch {
                if let Some(out_schema) = out_schema_opt {
                    if batch_tx_opt.is_none() {
                        let (btx, handle) = init_encoder(&lookup_params, flight_tx.clone());
                        batch_tx_opt = Some(btx);
                        encoder_handle = Some(handle);
                    }
                    let _ = batch_tx_opt
                        .as_ref()
                        .unwrap()
                        .send(Ok(RecordBatch::new_empty(out_schema)))
                        .await;
                }
            }

            drop(batch_tx_opt);
            if let Some(h) = encoder_handle {
                let _ = h.await;
            }

            if let Some(p) = &lookup_params {
                info!(
                    node_id,
                    namespace = %p.namespace,
                    set_name = %p.set_name,
                    join_type = ?p.join_type,
                    keys_requested = total_keys_req,
                    keys_found = total_keys_found,
                    batches_out = total_batches_out,
                    rows_out = total_rows_out,
                    elapsed_ms = stream_start.elapsed().as_millis() as u64,
                    "Completed Arrow Flight DoExchange lookup stream"
                );
            }
        });

        let output_stream = ReceiverStream::new(flight_rx).boxed();
        Ok(Response::new(output_stream))
    }
}

fn parse_job_id_from_body(body: &[u8]) -> Result<String, Status> {
    if let Ok(payload) = serde_json::from_slice::<JobStateActionPayload>(body) {
        if !payload.job_id.trim().is_empty() {
            return Ok(payload.job_id.trim().to_string());
        }
    }
    let s = std::str::from_utf8(body)
        .map_err(|_| Status::invalid_argument("Action body must be valid UTF-8 job_id or JSON"))?
        .trim();
    if s.is_empty() {
        return Err(Status::invalid_argument("job_id cannot be empty"));
    }
    Ok(s.to_string())
}

fn parse_job_state_payload(body: &[u8]) -> Result<JobStateActionPayload, Status> {
    if let Ok(payload) = serde_json::from_slice::<JobStateActionPayload>(body) {
        if !payload.job_id.trim().is_empty() {
            return Ok(payload);
        }
    }
    let job_id = parse_job_id_from_body(body)?;
    Ok(JobStateActionPayload {
        job_id,
        namespace: String::new(),
        set_name: String::new(),
        is_dq_validation: false,
        error: None,
    })
}
