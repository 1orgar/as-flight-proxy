# Direct Arrow Flight Operations from Python (`pyarrow.flight`)

> 🌐 **Language / Язык**: **[🇬🇧 English](../en/flight-operations.md)** | **[🇷🇺 Русский](../ru/flight-operations.md)**

Because `as_proxy_server` implements the standard **Apache Arrow Flight** gRPC protocol, you can interact with any proxy node directly from pure Python scripts, **Apache Airflow** / **Prefect** / **Dagster** DAGs, **Jupyter Notebooks**, or microservices using `pyarrow.flight` — without needing a Spark cluster.

This guide provides complete, copy-pasteable Python examples for every Arrow Flight operation:
1. [Connection, Authentication (`Handshake`), TLS/mTLS & Compression Headers](#1-connection-authentication-handshake-tlsmtls--compression-headers)
2. [Listing Supported Actions (`list_actions`)](#2-listing-supported-actions-list_actions)
3. [Inspecting Raft Cluster Status & Node Health (`DoAction: get_cluster_status`)](#3-inspecting-raft-cluster-status--node-health-doaction-get_cluster_status)
4. [Querying Job Status, DQ Reports & Resetting Job Metadata (`DoAction: get_job_status`, `init_job`)](#4-querying-job-status-dq-reports--resetting-job-metadata-doaction-get_job_status-init_job)
5. [Automatic Aerospike Set Schema Inference (`GetSchema`)](#5-automatic-aerospike-set-schema-inference-getschema)
6. [Reading Data from Aerospike into `pyarrow.Table` / `pandas.DataFrame` (`DoGet`)](#6-reading-data-from-aerospike-into-pyarrowtable--pandasdataframe-doget)
7. [Bidirectional Streaming Key-Lookup Enrichment (`DoExchange` Lookup)](#7-bidirectional-streaming-key-lookup-enrichment-doexchange-lookup)
8. [Direct Batch Writing & DQ Validation from Python (`DoPut`)](#8-direct-batch-writing--dq-validation-from-python-doput)

---

## 1. Connection, Authentication (`Handshake`), TLS/mTLS & Compression Headers

Install dependencies:
```bash
pip install pyarrow pandas
```

Reusable helper function that connects over `grpc://` or `grpc+tls://`, performs the `Handshake` (`authenticate_basic_token` $\rightarrow$ signed `Bearer` token), and builds `FlightCallOptions`:

```python
import base64
import json
from pathlib import Path
import pyarrow as pa
import pyarrow.flight as fl


def connect_flight_proxy(
    host_port: str,
    username: str = "",
    password: str = "",
    tls_enabled: bool = False,
    ca_cert_path: str | None = None,
    client_cert_path: str | None = None,
    client_key_path: str | None = None,
    verify_server: bool = True,
    compression: str = "zstd",   # "zstd", "lz4", "gzip", or "none"
    timeout_sec: float = 30.0,
) -> tuple[fl.FlightClient, fl.FlightCallOptions]:
    scheme = "grpc+tls" if tls_enabled else "grpc"
    location = f"{scheme}://{host_port}"

    client_kwargs = {}
    if tls_enabled:
        if ca_cert_path:
            client_kwargs["tls_root_certs"] = Path(ca_cert_path).read_bytes()
        if client_cert_path and client_key_path:
            client_kwargs["cert_chain"] = Path(client_cert_path).read_bytes()
            client_kwargs["private_key"] = Path(client_key_path).read_bytes()
        if not verify_server:
            client_kwargs["disable_server_verification"] = True

    client = fl.FlightClient(location, **client_kwargs)

    headers: list[tuple[bytes, bytes]] = []
    if username:
        # Handshake returns (b'authorization', b'Bearer <token>')
        bearer_header = client.authenticate_basic_token(username, password)
        headers.append(bearer_header)

    if compression:
        headers.append((b"x-as-compression", compression.encode("utf-8")))

    call_options = fl.FlightCallOptions(headers=headers, timeout=timeout_sec)
    return client, call_options
```

---

## 2. Listing Supported Actions (`list_actions`)

Calling `client.list_actions()` returns all Raft control actions supported by `as_proxy_server`:

```python
client, opts = connect_flight_proxy("as-flight-proxy-0.as-flight-proxy:3030", "user", "pass", tls_enabled=True, verify_server=False)
try:
    for action_info in client.list_actions(options=opts):
        print(f"{action_info.type:20s} -> {action_info.description}")
finally:
    client.close()
```

Output:
```text
init_job             -> Initialize or reset Raft-replicated metadata for a Spark job_id
get_job_status       -> Get Raft-replicated load status for a Spark job_id
get_cluster_status   -> Get Raft cluster topology, node metrics, and all jobs
complete_job         -> Mark a Spark load job_id as completed in Raft
fail_job             -> Mark a Spark load job_id as failed in Raft
```

---

## 3. Inspecting Raft Cluster Status & Node Health (`DoAction: get_cluster_status`)

The `get_cluster_status` action returns a complete snapshot of the Raft state machine: current role (`Leader` / `Follower`), `leader_id`, `current_term`, `commit_index`, live proxy node telemetry, and all active/recent jobs.

```python
def get_raft_cluster_status(client: fl.FlightClient, opts: fl.FlightCallOptions) -> dict:
    action = fl.Action("get_cluster_status", b"")
    results = list(client.do_action(action, options=opts))
    return json.loads(results[0].body.to_pybytes().decode("utf-8"))


client, opts = connect_flight_proxy("as-flight-proxy-0.as-flight-proxy:3030", "user", "pass", tls_enabled=True, verify_server=False)
try:
    cluster = get_raft_cluster_status(client, opts)
    print("Connected Node ID:", cluster["node_id"])
    print("Raft Role:", cluster["role"])                     # "Leader" | "Follower" | "Candidate"
    print("Current Leader ID:", cluster["leader_id"])
    print("Current Term:", cluster["current_term"])
    print("Commit Index:", cluster["commit_index"])
    print("Tracked Jobs in Raft:", len(cluster["state"]["jobs"]))
finally:
    client.close()
```

Sample `get_cluster_status` JSON payload:
```json
{
  "node_id": 1,
  "role": "Leader",
  "current_term": 3,
  "leader_id": 1,
  "commit_index": 42,
  "last_applied": 42,
  "state": {
    "nodes": {
      "1": {
        "node_id": 1,
        "flight_addr": "as-flight-proxy-0.as-flight-proxy:3030",
        "raft_addr": "as-flight-proxy-0.as-flight-proxy:8816",
        "aerospike_connected": true,
        "in_flight_records": 0,
        "records_written_total": 10000000,
        "updated_at_ms": 1762796800000
      }
    },
    "jobs": {
      "users-load-001": {
        "job_id": "users-load-001",
        "namespace": "dev",
        "set_name": "users",
        "is_dq_validation": false,
        "state": "completed",
        "total_rows_written": 10000000,
        "total_batches_processed": 9766,
        "total_overload_retries": 0,
        "total_other_retries": 0,
        "per_node_rows": { "1": 2500000, "2": 2500000, "3": 2500000, "4": 2500000 }
      }
    }
  }
}
```

---

## 4. Querying Job Status, DQ Reports & Resetting Job Metadata (`DoAction: get_job_status`, `init_job`)

### 4.1. Querying a Specific `job_id` (`get_job_status`)
Job status is replicated via Raft and can be queried from **any** proxy node within `raft.job_stats_ttl_secs`:

```python
def get_job_status(client: fl.FlightClient, opts: fl.FlightCallOptions, job_id: str) -> dict:
    action = fl.Action("get_job_status", job_id.encode("utf-8"))
    results = list(client.do_action(action, options=opts))
    return json.loads(results[0].body.to_pybytes().decode("utf-8"))


client, opts = connect_flight_proxy("as-flight-proxy-0.as-flight-proxy:3030", "user", "pass", tls_enabled=True, verify_server=False)
try:
    status = get_job_status(client, opts, "users-load-001")
    print("State:", status["state"])                         # "running" | "completed" | "failed"
    print("Rows Written:", status["total_rows_written"])
    print("Batches Processed:", status["total_batches_processed"])
    print("Overload Retries:", status["total_overload_retries"])
    print("Per-Node Row Distribution:", status["per_node_rows"])

    # If this was a DQ validation job (aerospike.dq = true):
    if status["is_dq_validation"]:
        print("DQ Passed:", status["dq_passed"])
        print("Rows Validated:", status["total_rows_validated"])
        print("Missing Keys:", status["total_missing_keys"])
        print("Bin Mismatches:", status["total_bin_mismatch_rows"])
        for sample in status.get("dq_mismatch_samples", []):
            print(f"  Mismatch on key {sample['key']}: {sample['reason']}")
finally:
    client.close()
```

### 4.2. Initializing / Overwriting Job Metadata in Raft (`init_job` / `reset_job`)
When a Spark job starts with an existing `connector.jobId`, the Spark Driver automatically invokes `init_job` to overwrite the old job metadata in Raft. You can also call `init_job` directly from Python:

```python
def reset_job_in_raft(
    client: fl.FlightClient,
    opts: fl.FlightCallOptions,
    job_id: str,
    namespace: str,
    set_name: str,
    is_dq_validation: bool = False,
) -> dict:
    payload = json.dumps({
        "job_id": job_id,
        "namespace": namespace,
        "set_name": set_name,
        "is_dq_validation": is_dq_validation,
    }).encode("utf-8")
    results = list(client.do_action(fl.Action("init_job", payload), options=opts))
    return json.loads(results[0].body.to_pybytes().decode("utf-8"))
```

### 4.3. Marking a Job as Completed or Failed (`complete_job` / `fail_job`)
```python
# Mark job as completed:
list(client.do_action(fl.Action("complete_job", b"users-load-001"), options=opts))

# Mark job as failed with an error message:
fail_payload = json.dumps({
    "job_id": "users-load-001",
    "namespace": "dev",
    "set_name": "users",
    "error": "Timeout waiting for upstream source"
}).encode("utf-8")
list(client.do_action(fl.Action("fail_job", fail_payload), options=opts))
```

---

## 5. Automatic Aerospike Set Schema Inference (`GetSchema`)

`client.get_schema(descriptor)` samples `sample_size` records from Aerospike and returns an inferred `pyarrow.Schema`:

```python
def infer_aerospike_schema(
    client: fl.FlightClient,
    opts: fl.FlightCallOptions,
    namespace: str,
    set_name: str,
    key_column: str = "id",
    sample_size: int = 1000,
) -> pa.Schema:
    cmd_bytes = json.dumps({
        "namespace": namespace,
        "set_name": set_name,
        "key_column": key_column,
        "sample_size": sample_size,
    }).encode("utf-8")
    descriptor = fl.FlightDescriptor.for_command(cmd_bytes)
    schema_result = client.get_schema(descriptor, options=opts)
    return schema_result.schema


client, opts = connect_flight_proxy("as-flight-proxy-0.as-flight-proxy:3030", "user", "pass", tls_enabled=True, verify_server=False)
try:
    arrow_schema = infer_aerospike_schema(client, opts, namespace="dev", set_name="users", key_column="id", sample_size=500)
    print(arrow_schema)
finally:
    client.close()
```

---

## 6. Reading Data from Aerospike into `pyarrow.Table` / `pandas.DataFrame` (`DoGet`)

To read via `DoGet`, serialize the target `pyarrow.Schema` as Arrow IPC Base64 and pass a `ReadTicket` JSON payload to `client.do_get(ticket)`.

Helper to encode `pyarrow.Schema` as `schema_ipc_base64`:
```python
def encode_schema_ipc_base64(schema: pa.Schema) -> str:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema) as writer:
        pass
    return base64.b64encode(sink.getvalue().to_pybytes()).decode("ascii")
```

### 6.1. Fast Primary-Key Batch Read (`BatchKeys`)
Fetches specific primary keys and selected bins in milliseconds via native Aerospike `BatchRead`:

```python
def read_keys_to_arrow(
    client: fl.FlightClient,
    opts: fl.FlightCallOptions,
    namespace: str,
    set_name: str,
    key_column: str,
    keys: list[str | int | bytes],
    target_schema: pa.Schema,
    batch_size: int = 1024,
    compression: str = "zstd",
) -> pa.Table:
    formatted_keys = []
    for k in keys:
        if isinstance(k, int):
            formatted_keys.append({"type": "int", "value": k})
        elif isinstance(k, bytes):
            formatted_keys.append({"type": "bytes", "value": base64.b64encode(k).decode("ascii")})
        else:
            formatted_keys.append({"type": "string", "value": str(k)})

    bins = [f.name for f in target_schema if f.name != key_column]
    ticket_payload = {
        "namespace": namespace,
        "set_name": set_name,
        "key_column": key_column,
        "bins": bins,
        "read_mode": {
            "mode": "batch_keys",
            "keys": formatted_keys,
        },
        "batch_size": batch_size,
        "type_mismatch_mode": "null",   # "fail" or "null"
        "schema_ipc_base64": encode_schema_ipc_base64(target_schema),
        "compression": compression,
    }
    ticket = fl.Ticket(json.dumps(ticket_payload).encode("utf-8"))
    reader = client.do_get(ticket, options=opts)
    return reader.read_all()


# Example usage:
target_schema = pa.schema([
    pa.field("id", pa.utf8(), nullable=False),
    pa.field("name", pa.utf8(), nullable=True),
    pa.field("score", pa.float64(), nullable=True),
])

client, opts = connect_flight_proxy("as-flight-proxy-0.as-flight-proxy:3030", "user", "pass", tls_enabled=True, verify_server=False)
try:
    table = read_keys_to_arrow(
        client,
        opts,
        namespace="dev",
        set_name="users",
        key_column="id",
        keys=["user_1", "user_2", "user_3"],
        target_schema=target_schema,
    )
    df_pandas = table.to_pandas()
    print(df_pandas)
finally:
    client.close()
```

### 6.2. Partition Range Scan (`PartitionRange`)
Aerospike namespaces have 4096 partitions (`0..4095`). You can scan either the entire set (`begin_partition=0, partition_count=4096`) or specific partition slices in parallel:

```python
def scan_partitions_to_arrow(
    client: fl.FlightClient,
    opts: fl.FlightCallOptions,
    namespace: str,
    set_name: str,
    key_column: str,
    target_schema: pa.Schema,
    begin_partition: int = 0,
    partition_count: int = 4096,
    batch_size: int = 4096,
    compression: str = "zstd",
) -> pa.Table:
    bins = [f.name for f in target_schema if f.name != key_column]
    ticket_payload = {
        "namespace": namespace,
        "set_name": set_name,
        "key_column": key_column,
        "bins": bins,
        "read_mode": {
            "mode": "partition_range",
            "begin_partition": begin_partition,
            "partition_count": partition_count,
        },
        "batch_size": batch_size,
        "type_mismatch_mode": "null",
        "schema_ipc_base64": encode_schema_ipc_base64(target_schema),
        "compression": compression,
    }
    ticket = fl.Ticket(json.dumps(ticket_payload).encode("utf-8"))
    reader = client.do_get(ticket, options=opts)
    return reader.read_all()
```

---

## 7. Bidirectional Streaming Key-Lookup Enrichment (`DoExchange` Lookup)

`client.do_exchange` opens a bidirectional stream: you stream `RecordBatch`es containing primary keys (plus any pass-through columns), and the proxy fetches the requested bins from Aerospike via `BatchRead` and streams back enriched `RecordBatch`es:

```python
def enrich_table_via_do_exchange(
    client: fl.FlightClient,
    opts: fl.FlightCallOptions,
    keys_table: pa.Table,
    namespace: str,
    set_name: str,
    key_column: str,
    bins_schema: pa.Schema,
    join_type: str = "left",        # "left" or "inner"
    batch_size: int = 4096,
    compression: str = "zstd",
) -> pa.Table:
    lookup_params = {
        "namespace": namespace,
        "set_name": set_name,
        "key_column": key_column,
        "join_type": join_type,
        "batch_size": batch_size,
        "type_mismatch_mode": "null",
        "bins_schema_ipc_base64": encode_schema_ipc_base64(bins_schema),
        "compression": compression,
    }
    descriptor = fl.FlightDescriptor.for_command(json.dumps(lookup_params).encode("utf-8"))

    writer, reader = client.do_exchange(descriptor, options=opts)
    with writer:
        writer.begin(keys_table.schema)
        for batch in keys_table.to_batches(max_chunksize=batch_size):
            writer.write_batch(batch)
        writer.done_writing()
        return reader.read_all()


# Example usage:
input_table = pa.table({
    "event_id": [101, 102, 103],
    "id": ["user_1", "user_missing", "user_3"],
})
bins_to_fetch = pa.schema([
    pa.field("name", pa.utf8(), nullable=True),
    pa.field("score", pa.float64(), nullable=True),
])

client, opts = connect_flight_proxy("as-flight-proxy-0.as-flight-proxy:3030", "user", "pass", tls_enabled=True, verify_server=False)
try:
    enriched_table = enrich_table_via_do_exchange(
        client,
        opts,
        keys_table=input_table,
        namespace="dev",
        set_name="users",
        key_column="id",
        bins_schema=bins_to_fetch,
        join_type="left",
    )
    print(enriched_table.to_pandas())
finally:
    client.close()
```

---

## 8. Direct Batch Writing & DQ Validation from Python (`DoPut`)

Using `client.do_put`, you can stream a `pyarrow.Table` / `pandas.DataFrame` directly into Aerospike or run a DQ validation (`validate_only=True`), receiving a `PutBatchAck` JSON response for every batch:

```python
def write_or_validate_table_via_do_put(
    client: fl.FlightClient,
    opts: fl.FlightCallOptions,
    table: pa.Table,
    job_id: str,
    namespace: str,
    set_name: str,
    key_column: str,
    write_mode: str = "update",     # "update", "replace", "update_only", "replace_only", "create_only"
    ttl_seconds: int | None = 86400,
    ignore_nulls: bool = False,
    validate_only: bool = False,    # True = DQ validation mode (does not modify Aerospike records)
    batch_size: int = 2048,
) -> list[dict]:
    # 1. Initialize/overwrite job_id metadata in Raft before streaming batches:
    reset_job_in_raft(client, opts, job_id, namespace, set_name, is_dq_validation=validate_only)

    write_params = {
        "job_id": job_id,
        "namespace": namespace,
        "set_name": set_name,
        "key_column": key_column,
        "write_mode": write_mode,
        "ttl_seconds": ttl_seconds,
        "send_key": True,
        "ignore_nulls": ignore_nulls,
        "use_batch_write": True,
        "validate_only": validate_only,
    }
    descriptor = fl.FlightDescriptor.for_command(json.dumps(write_params).encode("utf-8"))

    acks: list[dict] = []
    writer, reader = client.do_put(descriptor, table.schema, options=opts)
    with writer:
        for batch in table.to_batches(max_chunksize=batch_size):
            writer.write_batch(batch)
            ack_buf = reader.read()
            if ack_buf is not None:
                acks.append(json.loads(ack_buf.to_pybytes().decode("utf-8")))
        writer.done_writing()

    # 2. Mark job as completed in Raft:
    list(client.do_action(fl.Action("complete_job", job_id.encode("utf-8")), options=opts))
    return acks
```
