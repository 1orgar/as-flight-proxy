# Spark Connector Guide (`spark-aerospike-flight-connector`)

🇬🇧 **English** | [🇷🇺 Русская версия](../ru/spark-connector.md) | [⬅ Back to README](../../README.md)

---

## Table of Contents

1. [Installation & Spark Submit](#1-installation--spark-submit)
2. [Complete Connector Options Reference](#2-complete-connector-options-reference)
   - [2.1. Target & Schema Validation Options](#21-target--schema-validation-options)
   - [2.2. Arrow Flight Connection, Auth, TLS & Compression Options](#22-arrow-flight-connection-auth-tls--compression-options)
   - [2.3. Write & Data Quality (DQ) Options](#23-write--data-quality-dq-options)
   - [2.4. Read (Scan & BatchGet) Options](#24-read-scan--batchget-options)
   - [2.5. Distributed Key Lookup (`DoExchange`) Options](#25-distributed-key-lookup-doexchange-options)
3. [Data Type Mapping, `NULL` Semantics & `NaN` Handling](#3-data-type-mapping-null-semantics--nan-handling)
4. [Driver Schema & Bin Validation Rules (ФЛК)](#4-driver-schema--bin-validation-rules-флк)
5. [Code Examples (PySpark & Scala)](#5-code-examples-pyspark--scala)
   - [5.1. Batch & Streaming Writes](#51-batch--streaming-writes)
   - [5.2. Data Quality (DQ) Validation](#52-data-quality-dq-validation)
   - [5.3. Parallel Read: Partition Scan, Schema Inference & PK Pushdown](#53-parallel-read-partition-scan-schema-inference--pk-pushdown)
   - [5.4. Distributed DataFrame Key Lookup Enrichment (`DoExchange`)](#54-distributed-dataframe-key-lookup-enrichment-doexchange)
   - [5.5. Querying Job Status & Raft Cluster Status](#55-querying-job-status--raft-cluster-status)
6. [Optimal Option Profiles for Different Workloads](#6-optimal-option-profiles-for-different-workloads)

---

## 1. Installation & Spark Submit

Use the shaded assembly Fat-JAR `spark-aerospike-flight-connector-assembly-0.4.3.jar` with **Apache Spark 3.5.x** (Scala 2.12, Java 11/17):

```bash
# PySpark shell or spark-submit
pyspark \
  --jars /path/to/spark-aerospike-flight-connector-assembly-0.4.3.jar \
  --conf "spark.driver.extraJavaOptions=--add-opens=java.base/java.nio=ALL-UNNAMED" \
  --conf "spark.executor.extraJavaOptions=--add-opens=java.base/java.nio=ALL-UNNAMED"
```

Data source format short name: **`"aerospike-flight"`** (or full class name `"com.asproxy.spark.AerospikeFlightDataSource"`).

---

## 2. Complete Connector Options Reference

### 2.1. Target & Schema Validation Options

| Option | Aliases | Required | Default | Description |
| :--- | :--- | :---: | :---: | :--- |
| `aerospike.namespace` | — | **Yes** | — | Target Aerospike namespace (`1..31` ASCII chars) |
| `aerospike.set` | — | No | `""` | Target Aerospike set name (`0..63` ASCII chars) |
| `aerospike.keyColumn` | `aerospike.key_column` | **Yes** | — | Name of the primary key column in the Spark DataFrame |
| `aerospike.maxBinNameLen` | `aerospike.max_bin_name_len` | No | `15` | Maximum allowed bin name length in UTF-8 bytes (`1..15`) |
| `aerospike.maxBinsPerRecord` | `aerospike.max_bins_per_record` | No | `32767` | Maximum number of bin columns per record (`1..32767`) |

---

### 2.2. Arrow Flight Connection, Auth, TLS & Compression Options

| Option | Aliases | Default | Description |
| :--- | :--- | :---: | :--- |
| `flight.hosts` | `aerospike.flight.hosts`, `aerospike.flight.endpoint` | `"127.0.0.1:8815"` | Comma-separated list of proxy `host:port` endpoints (`"proxy1:8815,proxy2:8815,proxy3:8815"`) |
| `flight.username` | `aerospike.flight.username` | `""` | Username for Arrow Flight `Handshake` authentication |
| `flight.password` | `aerospike.flight.password` | `""` | Password for Arrow Flight `Handshake` authentication |
| `flight.timeoutMs` | `aerospike.flight.timeout_ms` | `"60000"` | Per-call Arrow Flight RPC timeout in milliseconds |
| `flight.compression` | `aerospike.flight.compression`, `compression` | `"zstd"` | Arrow Flight stream compression algorithm: `"zstd"` (default, off-heap IPC buffer compression; also supports inline level syntax like `"zstd:3"`), `"lz4"` (`LZ4_FRAME` IPC compression), `"gzip"` (gRPC transport compression), or `"none"` (`"off"`, `"false"`, `"disabled"`) |
| `flight.compression.level` | `flight.compressionLevel`, `aerospike.flight.compression.level`, `compression.level` | `"1"` | ZSTD compression level for outbound Spark batches: **`1..9`** standard range (`1` = fastest default, `3` = ZSTD balanced default, `9` = high ratio); also supports `10..22` (ultra) and `-7..-1` (ultra-fast) |
| `flight.compression.enabled` | `aerospike.flight.compression.enabled`, `compression.enabled` | `"true"` | Master client switch to enable (`"true"`) or disable (`"false"`) Flight compression |
| `flight.tls.enabled` | `aerospike.flight.tls.enabled` | `"false"` | Enables TLS (`grpc+tls://`) for Arrow Flight connections |
| `flight.tls.caCertPath` | `aerospike.flight.tls.ca_cert_path` | None | Path to trusted root CA certificate PEM file on Spark nodes |
| `flight.tls.clientCertPath` | `aerospike.flight.tls.client_cert_path` | None | Path to client certificate PEM file for Mutual TLS (mTLS) |
| `flight.tls.clientKeyPath` | `aerospike.flight.tls.client_key_path` | None | Path to client PKCS#8 private key PEM file for Mutual TLS (mTLS) |
| `flight.tls.verifyServer` | `aerospike.flight.tls.verify_server` | `"true"` | Verify server certificate hostname and chain when TLS is enabled |

---

### 2.3. Write & Data Quality (DQ) Options

| Option | Aliases | Default | Description |
| :--- | :--- | :---: | :--- |
| `connector.jobId` | `aerospike.jobId`, `aerospike.job_id`, `job.id` | **Required** | Unique job identifier used to track progress and DQ results in Raft (starting a job with an existing `jobId` automatically overwrites its metadata in Raft) |
| `aerospike.writeMode` | `aerospike.write_mode` | `"update"` | Write mode: `"update"`, `"replace"`, `"update_only"`, `"replace_only"`, `"create_only"`, `"validate"` (`"dq"`, `"verify"`) |
| `aerospike.dq` | `aerospike.validateOnly`, `aerospike.validate_only`, `connector.dq` | `"false"` | When `"true"`, runs in read-only Data Quality validation mode (`BatchRead` comparison against Aerospike) |
| `aerospike.ignoreNulls` | `aerospike.ignore_nulls` | `"false"` | When `"false"` (default), `NULL` bins in `update` mode emit `Value::Nil` to delete the bin in Aerospike. When `"true"`, `NULL` bins are omitted |
| `aerospike.ttl` | `aerospike.ttl_seconds` | Namespace default | Record expiration TTL in seconds (`0` = namespace default, `-1` = Never expire, `-2` = Do not update TTL) |
| `aerospike.sendKey` | `aerospike.send_key` | `"true"` | Store the raw user key Value alongside the RIPEMD-160 digest in Aerospike |
| `aerospike.useBatchWrite` | `aerospike.use_batch_write` | Proxy default | Override proxy's `writer.use_batch_write` (`"true"` or `"false"`) for this job |
| `connector.batchSize` | `aerospike.batchSize`, `aerospike.batch_size` | `"8192"` | Number of rows per Arrow `RecordBatch` on write |
| `connector.writerThreads` | `aerospike.writerThreads`, `aerospike.writer_threads` | `"4"` | Number of concurrent `DoPut` sender threads per Spark task |
| `connector.queueCapacity` | `aerospike.queueCapacity`, `aerospike.queue_capacity` | `max(writerThreads * 2, 4)` | Capacity of the per-task bounded `RecordBatch` queue |
| `connector.autoConvertStrings` | `aerospike.autoConvertStrings`, `connector.auto_convert_strings`, `aerospike.auto_convert_strings` | `"false"` | Automatic string bin conversion on write: `"1" -> Int(1)`, `"1.0" -> Double(1.0)`, non-numeric strings remain `String` |

---

### 2.4. Read (Scan & BatchGet) Options

| Option | Aliases | Default | Description |
| :--- | :--- | :---: | :--- |
| `aerospike.read.numPartitions` | `aerospike.read.num_partitions` | `"32"` | Number of Spark tasks to split the 4096 Aerospike partitions (or `BatchKeys`) across (`1..4096`) |
| `aerospike.read.batchSize` | `aerospike.read.batch_size` | `"4096"` | Number of records per Arrow `RecordBatch` streamed from proxy during `DoGet` / `DoExchange` |
| `aerospike.read.sampleSize` | `aerospike.read.sample_size` | `"1000"` | Maximum records sampled by `GetSchema` when `.schema(...)` is not provided |
| `aerospike.read.typeMismatchMode` | `aerospike.read.type_mismatch_mode` | `"fail"` | Behavior when a bin value in Aerospike cannot be converted to the target Spark column type: `"fail"` (`"strict"`, `"error"`) or `"null"` (`"permissive"`, `"ignore"`) |
| `aerospike.read.keys` | — | None | Optional explicit list of primary keys (CSV `"k1,k2,k3"` or JSON array `'["k1","k2"]'`) for direct `BatchKeys` lookup |

---

### 2.5. Distributed Key Lookup (`DoExchange`) Options

| Option | Aliases | Default | Description |
| :--- | :--- | :---: | :--- |
| `aerospike.lookup.joinType` | `aerospike.lookup.join_type` | `"left"` | Join behavior for `AerospikeFlightLookup`: `"left"` (`"left_outer"`, preserves all input rows with `null` bins for missing keys) or `"inner"` (emits only rows found in Aerospike) |
| `aerospike.lookup.sourceView` | `aerospike.lookup.source_view` | None | Temporary view name containing input keys when invoking `DoExchange` lookup via `spark.read.format("aerospike-flight")` |
| `aerospike.lookup.binsSchema` | `aerospike.lookup.bins_schema` | None | DDL (`"age INT, score DOUBLE"`) or JSON schema of bins to fetch when using `aerospike.lookup.sourceView` |

---

## 3. Data Type Mapping, `NULL` Semantics & `NaN` Handling

| Spark SQL Type | Apache Arrow Type | Aerospike `Value` | Notes |
| :--- | :--- | :--- | :--- |
| `BooleanType` | `Boolean` | `Value::Bool(bool)` | Also coerces `Int(0/1)` on read |
| `ByteType`, `ShortType`, `IntegerType`, `LongType` | `Int8`, `Int16`, `Int32`, `Int64` | `Value::Int(i64)` | Widened to signed 64-bit integer on write; range-checked on read |
| `FloatType`, `DoubleType` | `Float32`, `Float64` | `Value::Float(FloatValue)` | IEEE-754 64-bit float; **`NaN == NaN` is `true` in DQ validation** |
| `DecimalType(p, s)` | `Decimal128(p, s)` | `Value::String(String)` | Exact scale-preserving decimal string (e.g. `"12345.67"`) |
| `StringType` | `Utf8` | `Value::String(String)` (or `Int`/`Float` when `connector.autoConvertStrings=true`) | UTF-8 string; when `connector.autoConvertStrings=true`, `"1"` is stored as `Int(1)` and `"1.0"` as `Float(1.0)` |
| `BinaryType` | `Binary` | `Value::Blob(Vec<u8>)` | Raw byte array |
| `DateType` | `Date32` (days) | `Value::Int(i64)` | Stored as **UTC epoch milliseconds** (`days * 86_400_000`) |
| `TimestampType` | `Timestamp(Microsecond, "UTC")` | `Value::Int(i64)` | Stored as **UTC epoch milliseconds** (`micros / 1_000`) |
| `ArrayType(elemType)` | `List(elemField)` | `Value::List(Vec<Value>)` | Recursive Aerospike CDT List |
| `MapType(kType, vType)` | `Map(entries)` | `Value::OrderedMap(IndexMap)` | Recursive Aerospike CDT Map (`kType` must be scalar/non-float) |
| `StructType(fields)` | `Struct(fields)` | `Value::OrderedMap(IndexMap)` | Nested document (`field_name -> Value`) |
| `NullType` (`VOID`) | `Null` | `Value::Nil` | Supported for bin deletion in `update` mode when `ignoreNulls = false` |

### `NULL` Bin Semantics
* **`update` / `update_only` with `ignoreNulls = false` (default)**: A `NULL` column sends `Bin(name, Value::Nil)` which **deletes** that bin from the record in Aerospike.
* **`replace` / `replace_only` / `create_only`**: `NULL` columns are **never** sent over the wire because replacing a record in Aerospike automatically removes any bin not included in the write. Rows where all bin columns are `NULL` are skipped automatically.
* **`ignoreNulls = true`**: `NULL` columns are omitted in all write modes.

---

## 4. Driver Schema & Bin Validation Rules (ФЛК)

Before any job starts, `AerospikeSchemaValidator` validates all rules on the Driver and aggregates all violations into a single exception:
1. **Primary Key Column**: Must exist in the DataFrame and have type `StringType`, `ByteType`, `ShortType`, `IntegerType`, `LongType`, or `BinaryType`. Runtime `NULL` values in the PK column are rejected immediately.
2. **Bin Count**: Must have between `1` and `aerospike.maxBinsPerRecord` (default `32767`) bin columns.
3. **Bin Name Length & Characters**:
   * Length must be `1..15` UTF-8 bytes (`aerospike.maxBinNameLen`).
   * Must contain only printable ASCII characters (`0x21..0x7E`), with no whitespace, control characters, or reserved Aerospike delimiters (`:` or `;`).
   * No duplicate bin names.

---

## 5. Code Examples (PySpark & Scala)

### 5.1. Batch & Streaming Writes

**PySpark:**
```python
(
    df.write.format("aerospike-flight")
    .option("connector.jobId", "users-load-001")
    .option("flight.hosts", "proxy-1:8815,proxy-2:8815,proxy-3:8815")
    .option("flight.username", "spark_writer")
    .option("flight.password", "change_me_in_prod")
    .option("flight.compression", "zstd")           # "zstd" (default), "lz4", "gzip", or "none"
    .option("aerospike.namespace", "prod")
    .option("aerospike.set", "users")
    .option("aerospike.keyColumn", "user_id")
    .option("aerospike.writeMode", "replace")       # "update", "replace", "create_only", etc.
    .option("aerospike.ttl", "86400")
    .option("connector.batchSize", "8192")
    .option("connector.writerThreads", "4")
    .mode("append")
    .save()
)
```

---

### 5.2. Data Quality (DQ) Validation

```python
(
    df.write.format("aerospike-flight")
    .option("connector.jobId", "users-dq-001")
    .option("flight.hosts", "proxy-1:8815,proxy-2:8815,proxy-3:8815")
    .option("aerospike.namespace", "prod")
    .option("aerospike.set", "users")
    .option("aerospike.keyColumn", "user_id")
    .option("aerospike.dq", "true")                 # Read-only BatchRead comparison (NaN == NaN -> True)
    .mode("append")
    .save()
)

# Inspect driver-side summary directly from PySpark JVM:
summary_json = spark._jvm.com.asproxy.spark.AerospikeLoadStatus.latestSummaryJson()
print(summary_json)
```

---

### 5.3. Parallel Read: Partition Scan, Schema Inference & PK Pushdown

```python
# 1. Full 4096-partition parallel scan with automatic schema inference and column pruning
df_scan = (
    spark.read.format("aerospike-flight")
    .option("flight.hosts", "proxy-1:8815,proxy-2:8815,proxy-3:8815")
    .option("aerospike.namespace", "prod")
    .option("aerospike.set", "users")
    .option("aerospike.keyColumn", "user_id")
    .option("aerospike.read.numPartitions", "64")
    .option("aerospike.read.sampleSize", "1000")
    .load()
    .select("user_id", "age", "score")              # Only "age" and "score" bins are fetched!
)

# 2. Direct BatchGet via PK Filter Pushdown (=, <=>, IN, AND, OR)
df_keys = (
    spark.read.format("aerospike-flight")
    .schema("user_id STRING, age INT, score DOUBLE, tags ARRAY<STRING>")
    .option("flight.hosts", "proxy-1:8815,proxy-2:8815,proxy-3:8815")
    .option("aerospike.namespace", "prod")
    .option("aerospike.set", "users")
    .option("aerospike.keyColumn", "user_id")
    .load()
    .filter("user_id IN ('u_100', 'u_200', 'u_300')")
)
```

---

### 5.4. Distributed DataFrame Key Lookup Enrichment (`DoExchange`)

Enrich an existing Spark `DataFrame` containing primary keys (`keys_df`) by streaming partitions directly from Spark Executors to `as_proxy_server` over bidirectional Arrow Flight `DoExchange` (zero shuffle, zero driver collection):

**PySpark (via `AerospikeFlightLookup.lookup`):**
```python
from pyspark.sql import DataFrame

keys_df = spark.createDataFrame(
    [(101, "u_1"), (102, "u_2"), (103, "u_missing")],
    ["request_id", "user_id"],
)

lookup_options = {
    "flight.hosts": "proxy-1:8815,proxy-2:8815,proxy-3:8815",
    "flight.compression": "zstd",
    "aerospike.namespace": "prod",
    "aerospike.set": "users",
    "aerospike.keyColumn": "user_id",
    "aerospike.lookup.joinType": "left",            # "left" or "inner"
    "aerospike.read.batchSize": "4096",
}

jdf = spark._jvm.com.asproxy.spark.AerospikeFlightLookup.lookup(
    keys_df._jdf,
    "age INT, score DOUBLE, tags ARRAY<STRING>",
    lookup_options,
)
enriched_df = DataFrame(jdf, spark)
enriched_df.show()
```

**Scala:**
```scala
import com.asproxy.spark.AerospikeFlightLookup

val enrichedDf = AerospikeFlightLookup.lookup(
  keysDf = keysDf,
  binsSchemaDdlOrJson = "age INT, score DOUBLE, tags ARRAY<STRING>",
  options = Map(
    "flight.hosts" -> "proxy-1:8815,proxy-2:8815,proxy-3:8815",
    "aerospike.namespace" -> "prod",
    "aerospike.set" -> "users",
    "aerospike.keyColumn" -> "user_id",
    "aerospike.lookup.joinType" -> "left"
  )
)
```

---

### 5.5. Querying Job Status & Raft Cluster Status

**From PySpark / Scala (`AerospikeLoadStatus`):**
```python
# Query replicated job status from Raft on proxy_server:
job_json = spark._jvm.com.asproxy.spark.AerospikeLoadStatus.fetchRemoteJobStatusJson(
    "proxy-1:8815", "users-dq-001", "spark_writer", "change_me_in_prod", False
)
print(job_json)

# Query Raft cluster topology and leader status:
cluster_json = spark._jvm.com.asproxy.spark.AerospikeLoadStatus.fetchClusterStatusJson(
    "proxy-1:8815", "spark_writer", "change_me_in_prod", False
)
print(cluster_json)
```

---

## 6. Optimal Option Profiles for Different Workloads

Below are recommended connector and `as_proxy_server` parameter combinations for common production workloads (see [Section 5 of the Proxy Configuration Guide](proxy-configuration.md#5-performance-tuning--optimal-parameter-combinations-proxytoml--spark_connector) for server-side details).

| DataFrame & Cluster Profile | `connector.batchSize` | `connector.writerThreads` | `flight.compression` / `level` | Matching `proxy.toml` Settings |
| :--- | :---: | :---: | :---: | :--- |
| **Narrow DataFrame (`1–20` columns)**<br>Counters, segments, KV attributes | `8192` | `2..4` | `"zstd"` / `"1"` | `batch_write_size = 512..1024`<br>`max_in_flight_records = 16384`<br>`max_concurrent_subbatches_per_stream = 16` |
| **Wide DataFrame (`50–150` columns)**<br>e.g., `100` `Float`/`Double` columns on `160` Spark cores (`40` executors × `4` cores) | `2048..4096` | `2` | `"zstd"` / `"1"` | `batch_write_size = 512`<br>`max_in_flight_records = 32768`<br>`max_concurrent_subbatches_per_stream = 8`<br>`conn_pools_per_node = 8` |
| **Ultra-Wide DataFrame (`150–300+` columns)**<br>Wide ML feature tables or nested CDTs (`Array`/`Map`/`Struct`) | `1024..2048` | `1..2` | `"zstd"` / `"1..3"` | `max_decoding_message_size_mb = 128`<br>`batch_write_size = 256..512`<br>`max_in_flight_records = 32768`<br>`max_concurrent_subbatches_per_stream = 6..8` |
| **Small Proxy Pods (`1 vCPU / 2 GB RAM`)**<br>Under high Spark concurrency (`160+` active tasks) | `2048` | `1..2` | `"zstd"` (`"1"`) or `"lz4"` | `batch_write_size = 512`<br>`max_in_flight_records = 16384`<br>`max_concurrent_subbatches_per_stream = 4..8` |

### Example: High-Throughput Write of a Wide Feature Table (`100–300` columns) on 160 Spark Cores

```python
(
    df.write.format("aerospike-flight")
    .option("connector.jobId", "wide-features-300-bins-001")
    .option("flight.hosts", "proxy-1:8815,proxy-2:8815,proxy-3:8815,proxy-4:8815")
    .option("flight.compression", "zstd")
    .option("flight.compression.level", "1")        # Maximum off-heap ZSTD JNI speed
    .option("aerospike.namespace", "prod")
    .option("aerospike.set", "ml_features_wide")
    .option("aerospike.keyColumn", "entity_id")
    .option("aerospike.writeMode", "update")
    .option("aerospike.ignoreNulls", "true")        # Skip NULL bins on the wire
    .option("connector.batchSize", "2048")          # 2048 rows x 100..300 bins keeps Arrow batches compact
    .option("connector.writerThreads", "2")         # 160 cores x 2 threads = 320 streams (80 per proxy pod)
    .mode("append")
    .save()
)
```
