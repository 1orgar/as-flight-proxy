use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{error, info};

#[derive(Debug, Default)]
pub struct ProxyMetrics {
    pub records_written_total: AtomicU64,
    pub batches_processed_total: AtomicU64,
    pub overload_retries_total: AtomicU64,
    pub other_retries_total: AtomicU64,
    pub failed_records_total: AtomicU64,
    pub in_flight_records: AtomicI64,
    pub active_streams: AtomicI64,
    pub write_latency_micros_sum: AtomicU64,
    pub write_latency_count: AtomicU64,
}

impl ProxyMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn inc_records_written(&self, count: u64) {
        self.records_written_total.fetch_add(count, Ordering::Relaxed);
    }

    pub fn inc_batches_processed(&self) {
        self.batches_processed_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_overload_retries(&self, count: u64) {
        self.overload_retries_total.fetch_add(count, Ordering::Relaxed);
    }

    pub fn inc_other_retries(&self, count: u64) {
        self.other_retries_total.fetch_add(count, Ordering::Relaxed);
    }

    pub fn inc_failed_records(&self, count: u64) {
        self.failed_records_total.fetch_add(count, Ordering::Relaxed);
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

    pub fn render_prometheus(&self, aerospike_connected: bool) -> String {
        let records = self.records_written_total.load(Ordering::Relaxed);
        let batches = self.batches_processed_total.load(Ordering::Relaxed);
        let overload_retries = self.overload_retries_total.load(Ordering::Relaxed);
        let other_retries = self.other_retries_total.load(Ordering::Relaxed);
        let failed = self.failed_records_total.load(Ordering::Relaxed);
        let in_flight = self.in_flight_records.load(Ordering::Relaxed);
        let streams = self.active_streams.load(Ordering::Relaxed);
        let lat_sum_sec =
            self.write_latency_micros_sum.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let lat_count = self.write_latency_count.load(Ordering::Relaxed);
        let connected_val = u8::from(aerospike_connected);

        format!(
            "# HELP as_proxy_records_written_total Total number of records written to Aerospike\n\
             # TYPE as_proxy_records_written_total counter\n\
             as_proxy_records_written_total {records}\n\
             # HELP as_proxy_batches_processed_total Total number of Arrow RecordBatches processed\n\
             # TYPE as_proxy_batches_processed_total counter\n\
             as_proxy_batches_processed_total {batches}\n\
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
             # HELP as_proxy_active_streams Current number of active Arrow Flight DoPut streams\n\
             # TYPE as_proxy_active_streams gauge\n\
             as_proxy_active_streams {streams}\n\
             # HELP as_proxy_write_duration_seconds_sum Total time spent executing Aerospike write sub-batches\n\
             # TYPE as_proxy_write_duration_seconds_sum counter\n\
             as_proxy_write_duration_seconds_sum {lat_sum_sec:.6}\n\
             # HELP as_proxy_write_duration_seconds_count Total count of Aerospike write sub-batches\n\
             # TYPE as_proxy_write_duration_seconds_count counter\n\
             as_proxy_write_duration_seconds_count {lat_count}\n\
             # HELP as_proxy_aerospike_connected Whether the Aerospike client is connected to the cluster\n\
             # TYPE as_proxy_aerospike_connected gauge\n\
             as_proxy_aerospike_connected {connected_val}\n"
        )
    }
}

/// Spawn a lightweight HTTP observability server for `/metrics` and `/healthz`.
pub async fn serve_observability<F>(
    listen_addr: &str,
    metrics: Arc<ProxyMetrics>,
    health_check: F,
) -> std::io::Result<()>
where
    F: Fn() -> bool + Send + Sync + 'static,
{
    let listener = TcpListener::bind(listen_addr).await?;
    info!("Observability HTTP server listening on http://{listen_addr} (/metrics, /healthz)");
    let health_check = Arc::new(health_check);

    loop {
        let (mut socket, _) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                error!("Observability accept error: {e}");
                continue;
            }
        };

        let metrics = Arc::clone(&metrics);
        let health_check = Arc::clone(&health_check);

        tokio::spawn(async move {
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
                        ("HTTP/1.1 200 OK", "application/json", "{\"status\":\"ok\"}\n".to_string())
                    } else {
                        (
                            "HTTP/1.1 503 Service Unavailable",
                            "application/json",
                            "{\"status\":\"unhealthy\"}\n".to_string(),
                        )
                    }
                }
                "/metrics" => (
                    "HTTP/1.1 200 OK",
                    "text/plain; version=0.0.4; charset=utf-8",
                    metrics.render_prometheus(is_healthy),
                ),
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
        });
    }
}
