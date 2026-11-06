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
use crate::writer::MppWriteEngine;

/// JSON payload returned in `PutResult.app_metadata` after each `RecordBatch` is committed to Aerospike.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PutBatchAck {
    pub batch_index: usize,
    pub rows_written: usize,
    pub overload_retries: u64,
    pub other_retries: u64,
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
        }
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

        let first_msg = inbound.message().await?;
        let payload = first_msg
            .as_ref()
            .map(|m| m.payload.as_ref())
            .unwrap_or(&[]);

        let token = self
            .authenticator
            .authenticate_handshake(&metadata, payload)
            .map_err(Status::from)?;

        let bearer_header = format!("Bearer {token}");
        let header_val = MetadataValue::try_from(bearer_header.as_str())
            .map_err(|e| Status::internal(format!("Failed to encode Bearer header: {e}")))?;

        let response_msg = HandshakeResponse {
            protocol_version: first_msg.map(|m| m.protocol_version).unwrap_or(0),
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
                                    let _ = tx.send(Err(err.into())).await;
                                    return;
                                }
                            };

                        let stats = match write_engine.write_records(prepared, params).await {
                            Ok(s) => s,
                            Err(err) => {
                                error!("MPP write engine error on batch {batch_index}: {err}");
                                let _ = tx.send(Err(err.into())).await;
                                return;
                            }
                        };

                        total_stream_rows += stats.rows_written;
                        let ack = PutBatchAck {
                            batch_index,
                            rows_written: stats.rows_written,
                            overload_retries: stats.overload_retries,
                            other_retries: stats.other_retries,
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
                batches = batch_index,
                total_rows = total_stream_rows,
                "Completed Arrow Flight DoPut stream"
            );
        });

        let output_stream = ReceiverStream::new(rx).boxed();
        Ok(Response::new(output_stream))
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

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action is not supported"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions is not supported"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange is not supported"))
    }
}
