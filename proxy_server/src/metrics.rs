use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tracing::{error, info};

use crate::raft::{RaftNode, RaftRole};

#[derive(Debug, Default)]
pub struct ProxyMetrics {
    pub records_written_total: AtomicU64,
    pub batches_processed_total: AtomicU64,
    pub records_read_total: AtomicU64,
    pub read_batches_total: AtomicU64,
    pub lookup_keys_requested_total: AtomicU64,
    pub lookup_keys_found_total: AtomicU64,
    pub records_validated_total: AtomicU64,
    pub validation_mismatches_total: AtomicU64,
    pub overload_retries_total: AtomicU64,
    pub other_retries_total: AtomicU64,
    pub failed_records_total: AtomicU64,
    pub in_flight_records: AtomicI64,
    pub active_streams: AtomicI64,
    pub write_latency_micros_sum: AtomicU64,
    pub write_latency_count: AtomicU64,
    pub read_latency_micros_sum: AtomicU64,
    pub read_latency_count: AtomicU64,
    pub adaptive_batch_size: AtomicU64,
    pub adaptive_concurrency: AtomicU64,
    pub adaptive_pacing_micros: AtomicU64,
    pub adaptive_queue_inflation_permille: AtomicU64,
    pub cluster_wait_events_total: AtomicU64,
}

impl ProxyMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn inc_records_written(&self, count: u64) {
        self.records_written_total
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn inc_batches_processed(&self) {
        self.batches_processed_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_records_read(&self, count: u64) {
        self.records_read_total.fetch_add(count, Ordering::Relaxed);
    }

    pub fn inc_read_batches(&self) {
        self.read_batches_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_lookup_keys_requested(&self, count: u64) {
        self.lookup_keys_requested_total
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn inc_lookup_keys_found(&self, count: u64) {
        self.lookup_keys_found_total
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn inc_records_validated(&self, count: u64) {
        self.records_validated_total
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn inc_validation_mismatches(&self, count: u64) {
        self.validation_mismatches_total
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn inc_overload_retries(&self, count: u64) {
        self.overload_retries_total
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn inc_other_retries(&self, count: u64) {
        self.other_retries_total.fetch_add(count, Ordering::Relaxed);
    }

    pub fn inc_failed_records(&self, count: u64) {
        self.failed_records_total
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn add_in_flight(&self, delta: i64) {
        self.in_flight_records.fetch_add(delta, Ordering::Relaxed);
    }

    pub fn add_active_stream(&self, delta: i64) {
        self.active_streams.fetch_add(delta, Ordering::Relaxed);
    }

    pub fn observe_write_latency_micros(&self, micros: u64) {
        self.write_latency_micros_sum
            .fetch_add(micros, Ordering::Relaxed);
        self.write_latency_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn observe_read_latency_micros(&self, micros: u64) {
        self.read_latency_micros_sum
            .fetch_add(micros, Ordering::Relaxed);
        self.read_latency_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_adaptive_state(
        &self,
        batch_size: u64,
        concurrency: u64,
        pacing_micros: u64,
        queue_inflation_permille: u64,
    ) {
        self.adaptive_batch_size
            .store(batch_size, Ordering::Relaxed);
        self.adaptive_concurrency
            .store(concurrency, Ordering::Relaxed);
        self.adaptive_pacing_micros
            .store(pacing_micros, Ordering::Relaxed);
        self.adaptive_queue_inflation_permille
            .store(queue_inflation_permille, Ordering::Relaxed);
    }

    pub fn inc_cluster_wait_events(&self) {
        self.cluster_wait_events_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn render_prometheus(
        &self,
        aerospike_connected: bool,
        raft_info: Option<(u64, bool, u64)>,
    ) -> String {
        let records = self.records_written_total.load(Ordering::Relaxed);
        let batches = self.batches_processed_total.load(Ordering::Relaxed);
        let records_read = self.records_read_total.load(Ordering::Relaxed);
        let read_batches = self.read_batches_total.load(Ordering::Relaxed);
        let lookup_req = self.lookup_keys_requested_total.load(Ordering::Relaxed);
        let lookup_found = self.lookup_keys_found_total.load(Ordering::Relaxed);
        let records_validated = self.records_validated_total.load(Ordering::Relaxed);
        let validation_mismatches = self.validation_mismatches_total.load(Ordering::Relaxed);
        let overload_retries = self.overload_retries_total.load(Ordering::Relaxed);
        let other_retries = self.other_retries_total.load(Ordering::Relaxed);
        let failed = self.failed_records_total.load(Ordering::Relaxed);
        let in_flight = self.in_flight_records.load(Ordering::Relaxed);
        let streams = self.active_streams.load(Ordering::Relaxed);
        let lat_sum_sec =
            self.write_latency_micros_sum.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let lat_count = self.write_latency_count.load(Ordering::Relaxed);
        let read_lat_sum_sec =
            self.read_latency_micros_sum.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let read_lat_count = self.read_latency_count.load(Ordering::Relaxed);
        let adaptive_batch = self.adaptive_batch_size.load(Ordering::Relaxed);
        let adaptive_conc = self.adaptive_concurrency.load(Ordering::Relaxed);
        let adaptive_pacing = self.adaptive_pacing_micros.load(Ordering::Relaxed);
        let adaptive_q_inf = self
            .adaptive_queue_inflation_permille
            .load(Ordering::Relaxed);
        let wait_events = self.cluster_wait_events_total.load(Ordering::Relaxed);
        let connected_val = u8::from(aerospike_connected);
        let (raft_term, raft_is_leader, raft_commit_idx) = raft_info.unwrap_or((0, false, 0));
        let leader_val = u8::from(raft_is_leader);

        format!(
            "# HELP as_proxy_records_written_total Total number of records written to Aerospike\n\
             # TYPE as_proxy_records_written_total counter\n\
             as_proxy_records_written_total {records}\n\
             # HELP as_proxy_batches_processed_total Total number of Arrow RecordBatches processed on write/validate\n\
             # TYPE as_proxy_batches_processed_total counter\n\
             as_proxy_batches_processed_total {batches}\n\
             # HELP as_proxy_records_read_total Total number of records read from Aerospike (DoGet scan/batch and DoExchange)\n\
             # TYPE as_proxy_records_read_total counter\n\
             as_proxy_records_read_total {records_read}\n\
             # HELP as_proxy_read_batches_total Total number of Arrow RecordBatches emitted by DoGet and DoExchange\n\
             # TYPE as_proxy_read_batches_total counter\n\
             as_proxy_read_batches_total {read_batches}\n\
             # HELP as_proxy_lookup_keys_requested_total Total number of primary keys requested via DoExchange lookup\n\
             # TYPE as_proxy_lookup_keys_requested_total counter\n\
             as_proxy_lookup_keys_requested_total {lookup_req}\n\
             # HELP as_proxy_lookup_keys_found_total Total number of primary keys found in Aerospike during DoExchange lookup\n\
             # TYPE as_proxy_lookup_keys_found_total counter\n\
             as_proxy_lookup_keys_found_total {lookup_found}\n\
             # HELP as_proxy_records_validated_total Total number of records checked in DQ validateOnly mode\n\
             # TYPE as_proxy_records_validated_total counter\n\
             as_proxy_records_validated_total {records_validated}\n\
             # HELP as_proxy_validation_mismatches_total Total number of mismatched records detected in DQ validateOnly mode\n\
             # TYPE as_proxy_validation_mismatches_total counter\n\
             as_proxy_validation_mismatches_total {validation_mismatches}\n\
             # HELP as_proxy_overload_retries_total Total number of infinite-retry overload attempts (DEVICE_OVERLOAD, KEY_BUSY, NO_MORE_CONNECTIONS, MAX_RETRIES_EXCEEDED)\n\
             # TYPE as_proxy_overload_retries_total counter\n\
             as_proxy_overload_retries_total {overload_retries}\n\
             # HELP as_proxy_other_retries_total Total number of bounded retry attempts for non-overload errors\n\
             # TYPE as_proxy_other_retries_total counter\n\
             as_proxy_other_retries_total {other_retries}\n\
             # HELP as_proxy_failed_records_total Total number of records that failed after exhausting retries\n\
             # TYPE as_proxy_failed_records_total counter\n\
             as_proxy_failed_records_total {failed}\n\
             # HELP as_proxy_in_flight_records Current number of records in flight to Aerospike\n\
             # TYPE as_proxy_in_flight_records gauge\n\
             as_proxy_in_flight_records {in_flight}\n\
             # HELP as_proxy_active_streams Current number of active Arrow Flight streams (DoPut, DoGet, DoExchange)\n\
             # TYPE as_proxy_active_streams gauge\n\
             as_proxy_active_streams {streams}\n\
             # HELP as_proxy_write_duration_seconds_sum Total time spent executing Aerospike write sub-batches\n\
             # TYPE as_proxy_write_duration_seconds_sum counter\n\
             as_proxy_write_duration_seconds_sum {lat_sum_sec:.6}\n\
             # HELP as_proxy_write_duration_seconds_count Total count of Aerospike write sub-batches\n\
             # TYPE as_proxy_write_duration_seconds_count counter\n\
             as_proxy_write_duration_seconds_count {lat_count}\n\
             # HELP as_proxy_read_duration_seconds_sum Total time spent executing Aerospike read/lookup sub-batches\n\
             # TYPE as_proxy_read_duration_seconds_sum counter\n\
             as_proxy_read_duration_seconds_sum {read_lat_sum_sec:.6}\n\
             # HELP as_proxy_read_duration_seconds_count Total count of Aerospike read/lookup sub-batches\n\
             # TYPE as_proxy_read_duration_seconds_count counter\n\
             as_proxy_read_duration_seconds_count {read_lat_count}\n\
             # HELP as_proxy_adaptive_batch_size Dynamically tuned Aerospike sub-batch write size (records)\n\
             # TYPE as_proxy_adaptive_batch_size gauge\n\
             as_proxy_adaptive_batch_size {adaptive_batch}\n\
             # HELP as_proxy_adaptive_concurrency Dynamically tuned per-stream sub-batch write concurrency\n\
             # TYPE as_proxy_adaptive_concurrency gauge\n\
             as_proxy_adaptive_concurrency {adaptive_conc}\n\
             # HELP as_proxy_adaptive_pacing_micros Current micro-pacing delay (microseconds) applied to avoid Aerospike cluster wait\n\
             # TYPE as_proxy_adaptive_pacing_micros gauge\n\
             as_proxy_adaptive_pacing_micros {adaptive_pacing}\n\
             # HELP as_proxy_adaptive_queue_inflation_permille Observed Aerospike write latency inflation vs baseline RTT in permille (1000 = 1.0x, >1550 = cluster queue wait)\n\
             # TYPE as_proxy_adaptive_queue_inflation_permille gauge\n\
             as_proxy_adaptive_queue_inflation_permille {adaptive_q_inf}\n\
             # HELP as_proxy_cluster_wait_events_total Total proactive Aerospike cluster-wait avoidance or overload backoff adjustments\n\
             # TYPE as_proxy_cluster_wait_events_total counter\n\
             as_proxy_cluster_wait_events_total {wait_events}\n\
             # HELP as_proxy_aerospike_connected Whether the Aerospike client is connected to the cluster\n\
             # TYPE as_proxy_aerospike_connected gauge\n\
             as_proxy_aerospike_connected {connected_val}\n\
             # HELP as_proxy_raft_term Current Raft term of this proxy node\n\
             # TYPE as_proxy_raft_term gauge\n\
             as_proxy_raft_term {raft_term}\n\
             # HELP as_proxy_raft_is_leader Whether this proxy node is currently the Raft leader\n\
             # TYPE as_proxy_raft_is_leader gauge\n\
             as_proxy_raft_is_leader {leader_val}\n\
             # HELP as_proxy_raft_commit_index Highest Raft log index known to be committed\n\
             # TYPE as_proxy_raft_commit_index gauge\n\
             as_proxy_raft_commit_index {raft_commit_idx}\n"
        )
    }
}

/// Spawn a lightweight HTTP observability server for `/metrics` and `/healthz`.
pub async fn serve_observability<F>(
    listen_addr: &str,
    metrics: Arc<ProxyMetrics>,
    raft_node: Option<Arc<RaftNode>>,
    health_check: F,
) -> std::io::Result<()>
where
    F: Fn() -> bool + Send + Sync + 'static,
{
    let listener = TcpListener::bind(listen_addr).await?;
    info!("Observability HTTP server listening on http://{listen_addr} (/metrics, /healthz)");
    let health_check = Arc::new(health_check);
    let conn_semaphore = Arc::new(Semaphore::new(128));

    loop {
        let (mut socket, _) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                error!("Observability accept error: {e}");
                continue;
            }
        };
        let _ = socket.set_nodelay(true);
        let Ok(permit) = Arc::clone(&conn_semaphore).try_acquire_owned() else {
            continue;
        };

        let metrics = Arc::clone(&metrics);
        let raft_node = raft_node.clone();
        let health_check = Arc::clone(&health_check);

        tokio::spawn(async move {
            let _permit = permit;
            let _ = tokio::time::timeout(Duration::from_secs(5), async {
                let mut buf = [0u8; 1024];
                let n = match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => n,
                };

                let req = String::from_utf8_lossy(&buf[..n]);
                let path = req
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap_or("/");

                let is_healthy = health_check();

                let (status_line, content_type, body) = match path {
                    "/healthz" | "/health" | "/readyz" => {
                        if is_healthy {
                            (
                                "HTTP/1.1 200 OK",
                                "application/json",
                                "{\"status\":\"ok\"}\n".to_string(),
                            )
                        } else {
                            (
                                "HTTP/1.1 503 Service Unavailable",
                                "application/json",
                                "{\"status\":\"unhealthy\"}\n".to_string(),
                            )
                        }
                    }
                    "/metrics" => {
                        let raft_info = if let Some(ref r) = raft_node {
                            let snap = r.get_cluster_snapshot().await;
                            Some((
                                snap.current_term,
                                snap.role == RaftRole::Leader,
                                snap.commit_index,
                            ))
                        } else {
                            None
                        };
                        (
                            "HTTP/1.1 200 OK",
                            "text/plain; version=0.0.4; charset=utf-8",
                            metrics.render_prometheus(is_healthy, raft_info),
                        )
                    }
                    _ => (
                        "HTTP/1.1 404 Not Found",
                        "text/plain; charset=utf-8",
                        "Not Found. Available endpoints: /metrics, /healthz\n".to_string(),
                    ),
                };

                let response = format!(
                    "{status_line}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
            })
            .await;
        });
    }
}
