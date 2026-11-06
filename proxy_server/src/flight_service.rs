use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info};

use crate::auth::Authenticator;
use crate::config::WriteParameters;
use crate::converter::convert_record_batch;
use crate::error::ProxyError;
use crate::metrics::ProxyMetrics;
use crate::raft::{JobState, MetadataCommand, RaftNode, now_ms};
use crate::writer::MppWriteEngine;

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
        }
    }

    pub fn with_raft_node(mut self, raft_node: Arc<RaftNode>) -> Self {
        self.raft_node = Some(raft_node);
        self
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
                        let prepared =
                            match convert_record_batch(&batch, params, max_bin_name_len) {
                                Ok(records) => records,
                                Err(err) => {
                                    error!("RecordBatch conversion error: {err}");
                                    if let (Some(raft), Some(job_id)) =
                                        (&raft_node, &params.job_id)
                                    {
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

                        let stats = match write_engine.write_records(prepared, params).await {
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
                    "Unsupported action type '{other}'. Supported: get_job_status, get_cluster_status, complete_job, fail_job"
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
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema is not supported"))
    }

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get is not supported"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange is not supported"))
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
        error: None,
    })
}
