# Observability, Logging & Prometheus Metrics

🇬🇧 **English** | [🇷🇺 Русская версия](../ru/observability.md) | [⬅ Back to README](../../README.md)

---

## Table of Contents

1. [Spark Driver & Executor Structured Logging](#1-spark-driver--executor-structured-logging)
2. [Spark SQL UI Custom Metrics](#2-spark-sql-ui-custom-metrics)
3. [Proxy Server Prometheus Metrics (`:9090/metrics`)](#3-proxy-server-prometheus-metrics-9090metrics)
4. [Health Check Endpoint (`:9090/healthz`)](#4-health-check-endpoint-9090healthz)
5. [PromQL Queries & Alerting Rules](#5-promql-queries--alerting-rules)

---

## 1. Spark Driver & Executor Structured Logging

`spark_connector` emits structured `INFO` / `WARN` / `ERROR` logs via `org.apache.spark.internal.Logging` (Log4j2) with clear role and component prefixes so you can immediately distinguish Driver planning from Executor execution:

| Log Prefix | Location | Logged Information |
| :--- | :---: | :--- |
| `[Driver][ФЛК]` | Spark Driver | Schema validation results (`namespace.set`, `keyColumn`, `binsCount`, `writeMode`, `omitNullsInFlight`) or aggregated validation errors |
| `[Driver][Write]` | Spark Driver | Batch/streaming write initialization, partition count, throughput (`rows/sec`), `rowsWritten`, `batchesSent`, `overloadRetries`, `otherRetries`, and DQ validation results (`dqPassed`, mismatch counts) |
| `[Executor][Write]` | Spark Executor | Per-task initialization (`partition`, `task`, `compression`, `writerThreads`, assigned proxy `endpoints`), per-batch retry warnings, and task commit summary (`elapsedMs`, `rowsWritten`, `batchesSent`) |
| `[Driver][InferSchema]` | Spark Driver | Schema inference latency (`elapsedMs`), proxy endpoint used, `sampleSize`, and inferred DDL schema |
| `[Driver][ScanPlan]` | Spark Driver | Column pruning (`requiredColumns`), pushed PK filters (`pushed` vs `postScanFilters`), and partition plan (`4096-partition scan` vs `BatchKeys` vs `DoExchange Lookup`) |
| `[Executor][Read]` | Spark Executor | Opening/closing `DoGet` stream per partition, mode (`PartitionRange` or `BatchKeys`), `compression`, `rowsRead`, `batchesRead`, and `elapsedMs` |
| `[Driver][Lookup]` | Spark Driver | Planning `DoExchange` lookup (`keyColumn`, `joinType`, `inputColumns`, `lookupBins`, `outputSchema`) |
| `[Executor][Lookup]` | Spark Executor | Opening/closing `DoExchange` stream per partition, `compression`, `inputRowsSent`, `inputBatchesSent`, `outputRowsReceived`, `outputBatchesReceived`, and `elapsedMs` |

---

## 2. Spark SQL UI Custom Metrics

The connector registers custom metrics visible directly in the **Spark SQL UI** execution DAG:

### Write & DQ Metrics (`AerospikeFlightBatchWrite` / `StreamingWrite`)
* `as_rows_written` — Total Aerospike rows written via Arrow Flight
* `as_batches_sent` — Total Arrow `RecordBatch`es sent to `proxy_server`
* `as_overload_retries` — Total Aerospike overload retries (`DEVICE_OVERLOAD` / `KEY_BUSY`)
* `as_other_retries` — Total non-overload retries
* `as_rows_validated` — Total DQ rows validated
* `as_matched_rows` — Total DQ matched rows
* `as_mismatched_rows` — Total DQ mismatched rows

### Read Metrics (`AerospikeFlightScan`)
* `as_rows_read` — Total Aerospike rows read via Arrow Flight `DoGet`
* `as_batches_read` — Total Arrow `RecordBatch`es received from `proxy_server`

---

## 3. Proxy Server Prometheus Metrics (`:9090/metrics`)

Each `as_proxy_server` instance exposes **21 Prometheus metrics** on `GET http://<proxy-host>:9090/metrics`:

### Write Metrics (`DoPut`)
| Metric Name | Type | Description |
| :--- | :---: | :--- |
| `as_proxy_records_written_total` | `counter` | Total number of records successfully written to Aerospike |
| `as_proxy_batches_processed_total` | `counter` | Total number of Arrow `RecordBatch`es processed on write |
| `as_proxy_write_duration_seconds_sum` | `counter` | Cumulative time spent executing Aerospike write sub-batches (seconds) |
| `as_proxy_write_duration_seconds_count` | `counter` | Total number of Aerospike write sub-batch operations executed |

### Read & Lookup Metrics (`DoGet` / `DoExchange` / `GetSchema`)
| Metric Name | Type | Description |
| :--- | :---: | :--- |
| `as_proxy_records_read_total` | `counter` | Total number of records read from Aerospike (`DoGet` + `DoExchange`) |
| `as_proxy_read_batches_total` | `counter` | Total number of Arrow `RecordBatch`es streamed to clients on read/lookup |
| `as_proxy_lookup_keys_requested_total` | `counter` | Total number of primary keys requested via `DoExchange` lookup |
| `as_proxy_lookup_keys_found_total` | `counter` | Total number of primary keys found in Aerospike during `DoExchange` lookup |
| `as_proxy_read_duration_seconds_sum` | `counter` | Cumulative time spent executing Aerospike read/scan/lookup operations (seconds) |
| `as_proxy_read_duration_seconds_count` | `counter` | Total number of Aerospike read/scan/lookup operations executed |

### Data Quality (DQ) Validation Metrics
| Metric Name | Type | Description |
| :--- | :---: | :--- |
| `as_proxy_records_validated_total` | `counter` | Total number of records validated in DQ mode (`validate_only = true`) |
| `as_proxy_validation_mismatches_total` | `counter` | Total number of DQ validation mismatches (`MissingKey` + `BinMismatch`) |

### Retries, Errors & Concurrency Gauges
| Metric Name | Type | Description |
| :--- | :---: | :--- |
| `as_proxy_overload_retries_total` | `counter` | Total retries caused by Aerospike overload (`DEVICE_OVERLOAD`, `KEY_BUSY`, `NO_MORE_CONNECTIONS`, `MAX_RETRIES_EXCEEDED`) |
| `as_proxy_other_retries_total` | `counter` | Total retries caused by non-overload transient errors |
| `as_proxy_failed_records_total` | `counter` | Total records that permanently failed after exhausting `max_other_retries` |
| `as_proxy_in_flight_records` | `gauge` | Current number of records concurrently in flight to Aerospike |
| `as_proxy_active_streams` | `gauge` | Current number of active Arrow Flight streams (`DoPut`, `DoGet`, `DoExchange`) |

### Aerospike & Raft Cluster Health Metrics
| Metric Name | Type | Description |
| :--- | :---: | :--- |
| `as_proxy_aerospike_connected` | `gauge` | `1` if the proxy is connected to the Aerospike cluster, `0` otherwise |
| `as_proxy_raft_node_id` | `gauge` | Configured Raft `node_id` of this proxy server |
| `as_proxy_raft_term` | `gauge` | Current Raft election term |
| `as_proxy_raft_is_leader` | `gauge` | `1` if this proxy node is the current Raft Leader, `0` otherwise |
| `as_proxy_raft_commit_index` | `gauge` | Current Raft log commit index |

---

## 4. Health Check Endpoint (`:9090/healthz`)

* `GET http://<proxy-host>:9090/healthz`
  * Returns **`200 OK`** (`{"status":"ok"}`) when `engine.is_connected()` is `true`.
  * Returns **`503 Service Unavailable`** (`{"status":"unhealthy"}`) if the Aerospike client has lost connectivity to all cluster nodes.

---

## 5. PromQL Queries & Alerting Rules

```promql
# 1. Cluster-wide write throughput (records/sec)
sum(rate(as_proxy_records_written_total[1m]))

# 2. Cluster-wide read & lookup throughput (records/sec)
sum(rate(as_proxy_records_read_total[1m]))

# 3. Average Aerospike sub-batch write latency (ms)
1000 * sum(rate(as_proxy_write_duration_seconds_sum[1m])) / sum(rate(as_proxy_write_duration_seconds_count[1m]))

# 4. Lookup hit ratio (%)
100 * sum(rate(as_proxy_lookup_keys_found_total[5m])) / sum(rate(as_proxy_lookup_keys_requested_total[5m]))

# 5. Raft leader check (must equal 1 across the proxy cluster)
sum(as_proxy_raft_is_leader)
```
