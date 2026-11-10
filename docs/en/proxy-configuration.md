# Proxy Server Configuration Guide (`as_proxy_server`)

🇬🇧 **English** | [🇷🇺 Русская версия](../ru/proxy-configuration.md) | [⬅ Back to README](../../README.md)

---

## Table of Contents

1. [Configuration Overview](#1-configuration-overview)
2. [CLI Flags & Environment Variables](#2-cli-flags--environment-variables)
3. [Detailed `proxy.toml` Reference](#3-detailed-proxytoml-reference)
   - [`[server]` — Arrow Flight, HTTP/2 & Compression](#31-server--arrow-flight-http2--compression)
   - [`[raft]` — Cluster Consensus & Job Status TTL](#32-raft--cluster-consensus--job-status-ttl)
   - [`[tls]` — TLS & Mutual TLS (mTLS)](#33-tls--tls--mutual-tls-mtls)
   - [`[auth]` — Flight Handshake & Bearer Tokens](#34-auth--flight-handshake--bearer-tokens)
   - [`[aerospike]` — Aerospike Cluster Connection Pool](#35-aerospike--aerospike-cluster-connection-pool)
   - [`[writer]` — MPP Batching & Concurrency](#36-writer--mpp-batching--concurrency)
   - [`[retry]` — Overload & Error Retry Policies](#37-retry--overload--error-retry-policies)
4. [Complete 3-Node Production Cluster Example](#4-complete-3-node-production-cluster-example)
5. [Performance Tuning Recommendations](#5-performance-tuning-recommendations)

---

## 1. Configuration Overview

`as_proxy_server` loads its configuration in the following priority order (highest to lowest):
1. **CLI arguments** (e.g. `--listen-addr`, `--aerospike-hosts`)
2. **Environment variables** (e.g. `AS_PROXY_LISTEN_ADDR`, `AEROSPIKE_HOSTS`)
3. **TOML configuration file** (`--config /path/to/proxy.toml` or `AS_PROXY_CONFIG`)
4. **Built-in defaults** (`AppConfig::default()`)

---

## 2. CLI Flags & Environment Variables

| CLI Flag | Environment Variable | Description | Example |
| :--- | :--- | :--- | :--- |
| `-c`, `--config <PATH>` | `AS_PROXY_CONFIG` | Path to the `.toml` configuration file | `--config /etc/as_proxy/proxy.toml` |
| `--listen-addr <ADDR>` | `AS_PROXY_LISTEN_ADDR` | Override `server.listen_addr` (Arrow Flight gRPC bind address) | `0.0.0.0:8815` |
| `--raft-addr <ADDR>` | `AS_PROXY_RAFT_ADDR` | Override `raft.listen_addr` (Raft consensus bind address) | `0.0.0.0:8816` |
| `--node-id <ID>` | `AS_PROXY_NODE_ID` | Override `raft.node_id` (unique integer node ID `>= 1`) | `1` |
| `--aerospike-hosts <HOSTS>` | `AEROSPIKE_HOSTS` | Override `aerospike.hosts` (comma-separated `host:port` seed nodes) | `10.0.1.10:3000,10.0.1.11:3000` |
| *(logging)* | `RUST_LOG` | Log verbosity filter (`error`, `warn`, `info`, `debug`, `trace`) | `RUST_LOG=info,as_proxy_server=debug` |

---

## 3. Detailed `proxy.toml` Reference

### 3.1. `[server]` — Arrow Flight, HTTP/2 & Compression

| Parameter | Type | Default | Description |
| :--- | :---: | :---: | :--- |
| `listen_addr` | `String` | `"0.0.0.0:8815"` | Bind address and port for the Arrow Flight gRPC server |
| `metrics_addr` | `String` | `"0.0.0.0:9090"` | Bind address and port for the HTTP observability server (`/metrics` and `/healthz`) |
| `max_decoding_message_size_mb` | `usize` | `64` | Maximum incoming gRPC message size in MiB |
| `tcp_nodelay` | `bool` | `true` | Enables `TCP_NODELAY` (disables Nagle's algorithm) for lower latency |
| `http2_stream_window_size_mb` | `u32` | `16` | HTTP/2 per-stream flow control window size in MiB |
| `http2_connection_window_size_mb` | `u32` | `32` | HTTP/2 connection-level flow control window size in MiB |
| `compression_enabled` | `bool` | `true` | **Master switch** for Arrow Flight compression. When `false`, disables all IPC and gRPC compression regardless of client request |
| `compression` | `String` | `"zstd"` | Default compression algorithm when `compression_enabled = true` and the client does not specify one: `"zstd"`, `"lz4"`, `"gzip"`, or `"none"` |
| `compression_level` | `i32` | `1` | Default ZSTD compression level for outbound server streams (`DoGet` / `DoExchange`): **`1..9`** standard range (`1` = fastest default), full range `-7..22` |

```toml
[server]
listen_addr = "0.0.0.0:8815"
metrics_addr = "0.0.0.0:9090"
max_decoding_message_size_mb = 64
tcp_nodelay = true
http2_stream_window_size_mb = 16
http2_connection_window_size_mb = 32
compression_enabled = true
compression = "zstd"
compression_level = 1
```

---

### 3.2. `[raft]` — Cluster Consensus & Job Status TTL

| Parameter | Type | Default | Description |
| :--- | :---: | :---: | :--- |
| `enabled` | `bool` | `true` | Enables the embedded Raft state machine for cluster metadata and distributed Spark job tracking |
| `node_id` | `u64` | `1` | Unique node identifier within the proxy cluster (`1`, `2`, `3`, ...) |
| `listen_addr` | `String` | `"0.0.0.0:8816"` | Dedicated TCP bind address for Raft consensus RPCs |
| `election_timeout_min_ms` | `u64` | `150` | Minimum randomized election timeout in milliseconds |
| `election_timeout_max_ms` | `u64` | `300` | Maximum randomized election timeout in milliseconds |
| `heartbeat_interval_ms` | `u64` | `50` | Leader heartbeat interval in milliseconds |
| `job_stats_ttl_secs` | `u64` | `3600` | Time-to-live (in seconds) for Spark job statistics in Raft state before automatic eviction |
| `peers` | `Array<RaftPeer>` | `[]` | List of all proxy nodes in the cluster (`node_id`, `raft_addr`, `flight_addr`) |

```toml
[raft]
enabled = true
node_id = 1
listen_addr = "0.0.0.0:8816"
election_timeout_min_ms = 150
election_timeout_max_ms = 300
heartbeat_interval_ms = 50
job_stats_ttl_secs = 3600

[[raft.peers]]
node_id = 1
raft_addr = "10.0.0.1:8816"
flight_addr = "10.0.0.1:8815"

[[raft.peers]]
node_id = 2
raft_addr = "10.0.0.2:8816"
flight_addr = "10.0.0.2:8815"

[[raft.peers]]
node_id = 3
raft_addr = "10.0.0.3:8816"
flight_addr = "10.0.0.3:8815"
```

---

### 3.3. `[tls]` — TLS & Mutual TLS (mTLS)

| Parameter | Type | Default | Description |
| :--- | :---: | :---: | :--- |
| `enabled` | `bool` | `false` | Enables TLS encryption on the Arrow Flight gRPC server (`:8815`) |
| `cert_path` | `Option<String>` | `None` | Path to the server X.509 certificate in PEM format (required when `enabled = true`) |
| `key_path` | `Option<String>` | `None` | Path to the server private key in PEM format (required when `enabled = true`) |
| `client_ca_path` | `Option<String>` | `None` | Optional path to a client CA certificate PEM file; when set, enforces **Mutual TLS (mTLS)** client certificate verification |

```toml
[tls]
enabled = true
cert_path = "/etc/as_proxy/certs/server.crt"
key_path = "/etc/as_proxy/certs/server.key"
# Uncomment to require mTLS client certificates:
# client_ca_path = "/etc/as_proxy/certs/client-ca.crt"
```

---

### 3.4. `[auth]` — Flight Handshake & Bearer Tokens

| Parameter | Type | Default | Description |
| :--- | :---: | :---: | :--- |
| `enabled` | `bool` | `false` | Requires Arrow Flight `Handshake` authentication and `Bearer` token verification on all RPCs |
| `username` | `String` | `"admin"` | Expected username in `Handshake` (`BasicAuth`) |
| `password` | `String` | `"admin"` | Expected password in `Handshake` (verified in constant time using `subtle::ConstantTimeEq`) |
| `token_secret` | `String` | `"change-me-hmac-secret"` | HMAC-SHA256 secret key used to sign and verify stateless Bearer tokens (must be identical across all proxy nodes) |
| `token_ttl_secs` | `u64` | `3600` | Validity duration of issued Bearer tokens in seconds |

```toml
[auth]
enabled = true
username = "spark_writer"
password = "super_strong_password"
token_secret = "shared_cluster_hmac_sha256_secret_key"
token_ttl_secs = 86400
```

---

### 3.5. `[aerospike]` — Aerospike Cluster Connection Pool

| Parameter | Type | Default | Description |
| :--- | :---: | :---: | :--- |
| `hosts` | `String` | `"127.0.0.1:3000"` | Comma-separated list of Aerospike seed nodes (`"host1:3000,host2:3000"`) |
| `user` | `Option<String>` | `None` | Optional Aerospike user if security is enabled on Aerospike |
| `password` | `Option<String>` | `None` | Optional Aerospike password |
| `connect_timeout_ms` | `u64` | `5000` | Initial connection timeout to Aerospike nodes in milliseconds |
| `timeout_ms` | `u64` | `1000` | Per-operation socket timeout for `BatchWrite`, `BatchRead`, `Put`, and `Scan` in milliseconds |
| `min_conns_per_node` | `usize` | `16` | Minimum pre-warmed connections kept open per Aerospike node |
| `max_conns_per_node` | `usize` | `512` | Maximum synchronous/batch connections per Aerospike node |
| `conn_pools_per_node` | `usize` | `4` | Number of independent connection pools per Aerospike node to minimize lock contention |
| `max_bin_name_len` | `usize` | `64` | Maximum allowed bin name length enforced by the proxy (`1..64`; Spark connector defaults to Aerospike's 15-byte limit) |

```toml
[aerospike]
hosts = "10.10.0.11:3000,10.10.0.12:3000,10.10.0.13:3000"
connect_timeout_ms = 5000
timeout_ms = 2000
min_conns_per_node = 32
max_conns_per_node = 512
conn_pools_per_node = 4
max_bin_name_len = 15
```

---

### 3.6. `[writer]` — MPP Batching & Concurrency

| Parameter | Type | Default | Description |
| :--- | :---: | :---: | :--- |
| `use_batch_write` | `bool` | `true` | Uses Aerospike's native `Client::batch` (`BatchWrite`) API instead of single-key `Put` calls |
| `batch_write_size` | `usize` | `512` | Number of records grouped into a single Aerospike `BatchWrite` / `BatchRead` sub-batch |
| `max_in_flight_records` | `usize` | `16384` | Global semaphore limit on total records concurrently in flight to Aerospike across all active streams on this proxy node |
| `max_concurrent_subbatches_per_stream` | `usize` | `16` | Maximum number of sub-batches executed concurrently within a single `DoPut` stream |

```toml
[writer]
use_batch_write = true
batch_write_size = 512
max_in_flight_records = 16384
max_concurrent_subbatches_per_stream = 16
```

---

### 3.7. `[retry]` — Overload & Error Retry Policies

| Parameter | Type | Default | Description |
| :--- | :---: | :---: | :--- |
| `max_other_retries` | `u32` | `5` | Maximum retry attempts for **non-overload** errors (`DEVICE_OVERLOAD`, `KEY_BUSY`, `NO_AVAILABLE_CONNECTIONS_TO_NODE`, and `MAX_RETRIES_EXCEEDED` are always retried **indefinitely**) |
| `initial_backoff_ms` | `u64` | `10` | Initial backoff delay before the first retry in milliseconds |
| `max_backoff_ms` | `u64` | `2000` | Maximum cap on exponential backoff delay in milliseconds |
| `backoff_multiplier` | `f64` | `2.0` | Exponential growth factor per retry attempt (applied with full random jitter) |

```toml
[retry]
max_other_retries = 5
initial_backoff_ms = 10
max_backoff_ms = 2000
backoff_multiplier = 2.0
```

---

## 4. Complete 3-Node Production Cluster Example

To deploy a 3-node HA proxy cluster (`10.0.0.1`, `10.0.0.2`, `10.0.0.3`), use the same `proxy.toml` across all 3 nodes and pass `--node-id` via CLI or environment variable:

```bash
# Node 1 (10.0.0.1)
AS_PROXY_NODE_ID=1 ./as_proxy_server --config /etc/as_proxy/proxy.toml

# Node 2 (10.0.0.2)
AS_PROXY_NODE_ID=2 ./as_proxy_server --config /etc/as_proxy/proxy.toml

# Node 3 (10.0.0.3)
AS_PROXY_NODE_ID=3 ./as_proxy_server --config /etc/as_proxy/proxy.toml
```

---

## 5. Performance Tuning Recommendations

1. **`writer.batch_write_size`**:
   * For records with `5–20` scalar bins, `batch_write_size = 512` to `1024` yields optimal throughput (`400K+` rows/sec on 2 proxy nodes).
   * For large CDT (`Array`/`Map`/`Struct`) or `Binary` payloads (`> 10 KB` per record), lower `batch_write_size` to `128–256` to keep individual Aerospike batch payloads well below `batch-max-buffers-per-queue`.
2. **`aerospike.max_conns_per_node` & `conn_pools_per_node`**:
   * Ensure `max_conns_per_node >= (max_in_flight_records / batch_write_size) * 4`. Setting `conn_pools_per_node = 4` or `8` eliminates mutex contention across Tokio worker threads.
3. **`server.compression`**:
   * Keep `compression = "zstd"` (default) when network bandwidth between Spark executors and proxy nodes is shared or cross-AZ (`3x–6x` wire reduction at `< 3%` CPU cost thanks to off-heap `zstd-jni`).
   * Use `compression = "lz4"` for ultra-low CPU overhead, or `compression_enabled = false` on local 100GbE networks where CPU is the only bottleneck.
