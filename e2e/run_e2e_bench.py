#!/usr/bin/env python3
"""
End-to-End (E2E) Benchmark Suite:
  Spark 3.5 -> Arrow Flight -> 2x as_proxy_server (Rust) -> 2x Aerospike 8.0 CE
Measures throughput (rows/sec, MB/sec) and latency across different:
  - Proxy cluster topologies (1 proxy node vs 2 proxy nodes)
  - Aerospike write engines (BatchWrite vs Single-Put)
  - Arrow RecordBatch sizes (512, 2048, 8192, 16384)
  - Spark partitions & writerThreads per task
  - Modes (Update, Replace, DQ Validation)
  - Payload schemas (Flat primitives vs Nested CDT Array/Map/Struct)
"""

import json
import time
import pyarrow.flight as fl
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

USERNAME = "spark_loader"
PASSWORD = "super_secret_password"
NAMESPACE = "test"


def flight_action(endpoint: str, action_type: str, body: bytes = b"") -> dict:
    client = fl.FlightClient(f"grpc://{endpoint}")
    auth_token = client.authenticate_basic_token(USERNAME, PASSWORD)
    options = fl.FlightCallOptions(headers=[auth_token], timeout=10.0)
    action = fl.Action(action_type, body)
    results = list(client.do_action(action, options=options))
    if not results:
        return {}
    return json.loads(results[0].body.to_pybytes().decode("utf-8"))


def wait_for_cluster_ready(timeout_secs: int = 30):
    deadline = time.time() + timeout_secs
    while time.time() < deadline:
        try:
            snap1 = flight_action("proxy-1:8815", "get_cluster_status")
            snap2 = flight_action("proxy-2:8815", "get_cluster_status")
            nodes1 = snap1.get("state", {}).get("nodes", {})
            if (
                snap1.get("leader_id") is not None
                and snap1.get("leader_id") == snap2.get("leader_id")
                and len(nodes1) >= 2
                and all(n.get("aerospike_connected") for n in nodes1.values())
            ):
                return
        except Exception:
            pass
        time.sleep(0.5)
    raise RuntimeError("Cluster did not become ready in time")


def build_flat_df(spark: SparkSession, num_rows: int, partitions: int):
    df = (
        spark.range(0, num_rows, 1, partitions)
        .select(
            F.concat(F.lit("k_"), F.col("id").cast("string")).alias("pk"),
            (F.col("id") % 100).cast("int").alias("age"),
            (F.col("id") * 1.25).cast("double").alias("balance"),
            ((F.col("id") % 2) == 0).alias("active"),
            F.concat(F.lit("user_payload_string_"), F.col("id").cast("string")).alias("name"),
            F.current_timestamp().alias("ts"),
        )
        .cache()
    )
    df.count()  # Materialize in Spark memory before timing write
    return df


def build_nested_cdt_df(spark: SparkSession, num_rows: int, partitions: int):
    df = (
        spark.range(0, num_rows, 1, partitions)
        .select(
            F.concat(F.lit("cdt_"), F.col("id").cast("string")).alias("pk"),
            (F.col("id") % 100).cast("int").alias("age"),
            F.when((F.col("id") % 20) == 0, F.lit(float("nan")))
            .otherwise((F.col("id") * 1.5).cast("double"))
            .alias("balance"),
            F.array(F.lit("tag_a"), F.lit("tag_b"), F.col("id").cast("string")).alias("tags"),
            F.create_map(
                F.lit("s1"),
                (F.col("id") * 0.5).cast("double"),
                F.lit("s2"),
                F.lit(100.0),
            ).alias("scores"),
            F.struct(
                F.lit("gold").alias("tier"),
                (F.col("id") * 10).cast("long").alias("pts"),
            ).alias("profile"),
        )
        .cache()
    )
    df.count()
    return df


def run_single_bench(
    df,
    case_id: str,
    description: str,
    num_rows: int,
    partitions: int,
    writer_threads: int,
    batch_size: int,
    endpoints: str,
    use_batch_write: bool,
    write_mode: str,
    dq: bool,
    set_name: str,
) -> dict:
    job_id = f"bench-{case_id}-{int(time.time() * 1000)}"
    t0 = time.perf_counter()
    (
        df.write.format("aerospike-flight")
        .mode("append")
        .option("flight.hosts", endpoints)
        .option("flight.username", USERNAME)
        .option("flight.password", PASSWORD)
        .option("aerospike.namespace", NAMESPACE)
        .option("aerospike.set", set_name)
        .option("aerospike.keyColumn", "pk")
        .option("aerospike.writeMode", write_mode)
        .option("aerospike.useBatchWrite", str(use_batch_write).lower())
        .option("aerospike.dq", str(dq).lower())
        .option("aerospike.ignoreNulls", "false")
        .option("connector.jobId", job_id)
        .option("connector.batchSize", str(batch_size))
        .option("connector.writerThreads", str(writer_threads))
        .save()
    )
    elapsed_s = time.perf_counter() - t0
    rps = int(round(num_rows / elapsed_s))

    # Fetch Raft job status
    first_host = endpoints.split(",")[0]
    raft_status = {}
    for _ in range(10):
        try:
            raft_status = flight_action(first_host, "get_job_status", job_id.encode("utf-8"))
            if raft_status.get("state") == "completed":
                break
        except Exception:
            pass
        time.sleep(0.1)

    per_node = raft_status.get("per_node_rows", {})
    retries = raft_status.get("total_overload_retries", 0) + raft_status.get("total_other_retries", 0)
    batches = raft_status.get("total_batches_processed", 0)

    res = {
        "case": case_id,
        "description": description,
        "rows": num_rows,
        "proxies": len(endpoints.split(",")),
        "partitions": partitions,
        "threads": writer_threads,
        "batch_size": batch_size,
        "batch_write": use_batch_write,
        "mode": "DQ (Validate)" if dq else write_mode,
        "elapsed_s": round(elapsed_s, 3),
        "rps": rps,
        "batches": batches,
        "retries": retries,
        "per_node": per_node,
    }
    print(
        f"  -> [{case_id}] {description}: {num_rows:,} rows in {elapsed_s:.3f}s "
        f"= {rps:,} rows/sec (nodes={per_node}, batches={batches}, retries={retries})"
    )
    return res


def main():
    wait_for_cluster_ready()

    spark = (
        SparkSession.builder.appName("AerospikeFlightProxyBenchmark")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("\n=== Warming up JVM, Arrow allocator & gRPC channels ===")
    warmup_df = build_flat_df(spark, 10_000, 4)
    run_single_bench(
        warmup_df,
        "WARMUP",
        "Warmup run",
        10_000,
        4,
        2,
        2048,
        "proxy-1:8815,proxy-2:8815",
        True,
        "update",
        False,
        "bench_warmup",
    )
    warmup_df.unpersist()

    print("\n=== Materializing Benchmark Datasets in Spark Memory ===")
    df_100k_p2 = build_flat_df(spark, 100_000, 2)
    df_100k_p4 = build_flat_df(spark, 100_000, 4)
    df_100k_p8 = build_flat_df(spark, 100_000, 8)
    df_100k_cdt = build_nested_cdt_df(spark, 100_000, 4)
    df_250k_p8 = build_flat_df(spark, 250_000, 8)

    results = []

    print("\n=== Running E2E Benchmark Matrix ===")

    # 1. Single-Put vs Native BatchWrite (100k rows)
    results.append(
        run_single_bench(
            df_100k_p4,
            "B01",
            "Single-Put (useBatchWrite=false)",
            100_000,
            4,
            2,
            4096,
            "proxy-1:8815,proxy-2:8815",
            False,
            "update",
            False,
            "bench_flat",
        )
    )
    results.append(
        run_single_bench(
            df_100k_p4,
            "B02",
            "BatchWrite (useBatchWrite=true)",
            100_000,
            4,
            2,
            4096,
            "proxy-1:8815,proxy-2:8815",
            True,
            "update",
            False,
            "bench_flat",
        )
    )

    # 2. Effect of Proxy Cluster Scaling (1 Proxy Node vs 2 Proxy Nodes)
    results.append(
        run_single_bench(
            df_100k_p4,
            "B03",
            "1 Proxy Node (Single Node)",
            100_000,
            4,
            2,
            8192,
            "proxy-1:8815",
            True,
            "update",
            False,
            "bench_flat",
        )
    )
    results.append(
        run_single_bench(
            df_100k_p4,
            "B04",
            "2 Proxy Nodes (MPP Cluster)",
            100_000,
            4,
            2,
            8192,
            "proxy-1:8815,proxy-2:8815",
            True,
            "update",
            False,
            "bench_flat",
        )
    )

    # 3. Effect of Arrow Flight Batch Size (512 vs 2048 vs 8192 vs 16384)
    results.append(
        run_single_bench(
            df_100k_p4,
            "B05",
            "Small Arrow Batch (batchSize=512)",
            100_000,
            4,
            2,
            512,
            "proxy-1:8815,proxy-2:8815",
            True,
            "update",
            False,
            "bench_flat",
        )
    )
    results.append(
        run_single_bench(
            df_100k_p4,
            "B06",
            "Medium Arrow Batch (batchSize=2048)",
            100_000,
            4,
            2,
            2048,
            "proxy-1:8815,proxy-2:8815",
            True,
            "update",
            False,
            "bench_flat",
        )
    )
    results.append(
        run_single_bench(
            df_100k_p4,
            "B07",
            "Large Arrow Batch (batchSize=16384)",
            100_000,
            4,
            2,
            16384,
            "proxy-1:8815,proxy-2:8815",
            True,
            "update",
            False,
            "bench_flat",
        )
    )

    # 4. Effect of Spark Partitions & Writer Threads
    results.append(
        run_single_bench(
            df_100k_p2,
            "B08",
            "Low Parallelism (2 part x 1 thread)",
            100_000,
            2,
            1,
            8192,
            "proxy-1:8815,proxy-2:8815",
            True,
            "update",
            False,
            "bench_flat",
        )
    )
    results.append(
        run_single_bench(
            df_100k_p8,
            "B09",
            "High Parallelism (8 part x 2 threads)",
            100_000,
            8,
            2,
            8192,
            "proxy-1:8815,proxy-2:8815",
            True,
            "update",
            False,
            "bench_flat",
        )
    )
    results.append(
        run_single_bench(
            df_100k_p8,
            "B10",
            "Max Parallelism (8 part x 4 threads)",
            100_000,
            8,
            4,
            8192,
            "proxy-1:8815,proxy-2:8815",
            True,
            "update",
            False,
            "bench_flat",
        )
    )

    # 5. Write Mode: Replace vs Update vs DQ Validation
    results.append(
        run_single_bench(
            df_100k_p8,
            "B11",
            "Replace Mode (writeMode=replace)",
            100_000,
            8,
            2,
            8192,
            "proxy-1:8815,proxy-2:8815",
            True,
            "replace",
            False,
            "bench_flat",
        )
    )
    results.append(
        run_single_bench(
            df_100k_p8,
            "B12",
            "DQ Validation Mode (dq=true, Flat)",
            100_000,
            8,
            2,
            8192,
            "proxy-1:8815,proxy-2:8815",
            True,
            "update",
            True,
            "bench_flat",
        )
    )

    # 6. Complex Nested CDT Schema (Array + Map + Struct + NaN) Write & DQ
    results.append(
        run_single_bench(
            df_100k_cdt,
            "B13",
            "Complex CDT Write (List+Map+Struct)",
            100_000,
            4,
            2,
            8192,
            "proxy-1:8815,proxy-2:8815",
            True,
            "update",
            False,
            "bench_cdt",
        )
    )
    results.append(
        run_single_bench(
            df_100k_cdt,
            "B14",
            "Complex CDT DQ Validate (NaN+CDTs)",
            100_000,
            4,
            2,
            8192,
            "proxy-1:8815,proxy-2:8815",
            True,
            "update",
            True,
            "bench_cdt",
        )
    )

    # 7. Sustained 250,000 Rows High-Throughput Load
    results.append(
        run_single_bench(
            df_250k_p8,
            "B15",
            "Sustained Load (250k rows, 8p x 4t)",
            250_000,
            8,
            4,
            8192,
            "proxy-1:8815,proxy-2:8815",
            True,
            "replace",
            False,
            "bench_250k",
        )
    )

    print("\n=== BENCHMARK_RESULTS_JSON_START ===")
    print(json.dumps(results, indent=2))
    print("=== BENCHMARK_RESULTS_JSON_END ===\n")

    spark.stop()


if __name__ == "__main__":
    main()
