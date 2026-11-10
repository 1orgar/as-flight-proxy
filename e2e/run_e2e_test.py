#!/usr/bin/env python3
"""
End-to-End (E2E) Test Suite:
  PySpark (Spark 3.5) -> Spark Connector Fat-JAR ->
  2x as_proxy_server (Arrow Flight + Raft) ->
  2x Aerospike 8.0 CE Mesh Cluster
"""

import datetime
import json
import math
import sys
import time
import pyarrow.flight as fl
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

PROXY_ENDPOINTS = ["proxy-1:8815", "proxy-2:8815"]
USERNAME = "spark_loader"
PASSWORD = "super_secret_password"
NAMESPACE = "test"
SET_NAME = "e2e_users"


def flight_action(endpoint: str, action_type: str, body: bytes = b"") -> dict:
    client = fl.FlightClient(f"grpc://{endpoint}")
    auth_token = client.authenticate_basic_token(USERNAME, PASSWORD)
    options = fl.FlightCallOptions(headers=[auth_token], timeout=10.0)
    action = fl.Action(action_type, body)
    results = list(client.do_action(action, options=options))
    if not results:
        return {}
    return json.loads(results[0].body.to_pybytes().decode("utf-8"))


def wait_for_cluster_ready(timeout_secs: int = 30) -> dict:
    print("\n=== [Step 1] Waiting for 2-node Proxy Raft Cluster & 2-node Aerospike Cluster ===")
    deadline = time.time() + timeout_secs
    last_err = None
    while time.time() < deadline:
        try:
            snap1 = flight_action("proxy-1:8815", "get_cluster_status")
            snap2 = flight_action("proxy-2:8815", "get_cluster_status")
            leader1 = snap1.get("leader_id")
            leader2 = snap2.get("leader_id")
            nodes1 = snap1.get("state", {}).get("nodes", {})
            nodes2 = snap2.get("state", {}).get("nodes", {})
            if (
                leader1 is not None
                and leader1 == leader2
                and len(nodes1) >= 2
                and len(nodes2) >= 2
                and all(n.get("aerospike_connected") for n in nodes1.values())
            ):
                print(f"  [OK] Raft Leader elected: node_id={leader1}, term={snap1.get('current_term')}")
                for nid, nmeta in sorted(nodes1.items()):
                    print(
                        f"  [OK] Proxy Node {nid}: flight={nmeta['flight_addr']}, "
                        f"raft={nmeta['raft_addr']}, aerospike_connected={nmeta['aerospike_connected']}"
                    )
                return snap1
        except Exception as e:
            last_err = e
        time.sleep(1.0)
    raise RuntimeError(f"Proxy Raft cluster did not become ready within {timeout_secs}s: {last_err}")


def wait_for_job_status(endpoint: str, job_id: str, expected_state: str = "completed", timeout_secs: int = 10) -> dict:
    deadline = time.time() + timeout_secs
    last_status = {}
    while time.time() < deadline:
        try:
            status = flight_action(endpoint, "get_job_status", job_id.encode("utf-8"))
            last_status = status
            if status.get("state") == expected_state:
                return status
        except Exception:
            pass
        time.sleep(0.3)
    return last_status


def build_test_rows(num_rows: int = 500):
    base_date = datetime.date(2026, 1, 1)
    base_ts = datetime.datetime(2026, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
    rows = []
    for i in range(num_rows):
        # Every 10th row has NaN in top-level float and inside nested map
        is_nan_row = (i % 10 == 0)
        balance = float("nan") if is_nan_row else round(i * 12.5 + 0.25, 4)
        score_val = float("nan") if is_nan_row else float(i * 2)
        # Every 5th row has NULL in nullable string column
        note = None if (i % 5 == 0) else f"note_for_user_{i}"

        rows.append(
            (
                f"user_{i:04d}",
                20 + (i % 50),
                balance,
                (i % 2 == 0),
                base_date + datetime.timedelta(days=i % 30),
                base_ts + datetime.timedelta(seconds=i * 60),
                [f"tag_{i % 3}", "common"],
                {"primary": score_val, "bonus": 10.0},
                {"tier": "gold" if i % 3 == 0 else "silver", "points": int(i * 100)},
                note,
            )
        )
    return rows


def main():
    wait_for_cluster_ready()

    spark = (
        SparkSession.builder.appName("AerospikeFlightProxyE2E")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType(
        [
            StructField("user_id", StringType(), nullable=False),
            StructField("age", IntegerType(), nullable=False),
            StructField("balance", DoubleType(), nullable=False),
            StructField("is_active", BooleanType(), nullable=False),
            StructField("signup_date", DateType(), nullable=False),
            StructField("last_login", TimestampType(), nullable=False),
            StructField("tags", ArrayType(StringType(), containsNull=False), nullable=False),
            StructField("scores", MapType(StringType(), DoubleType(), valueContainsNull=False), nullable=False),
            StructField(
                "profile",
                StructType(
                    [
                        StructField("tier", StringType(), nullable=False),
                        StructField("points", LongType(), nullable=False),
                    ]
                ),
                nullable=False,
            ),
            StructField("note", StringType(), nullable=True),
        ]
    )

    total_rows = 500
    rows = build_test_rows(total_rows)
    df = spark.createDataFrame(rows, schema=schema).repartition(4)

    # -------------------------------------------------------------------------
    # STEP 2: Parallel Write across 2 Proxy Nodes -> 2 Aerospike Nodes
    # -------------------------------------------------------------------------
    write_job_id = "e2e-write-job-001"
    print(f"\n=== [Step 2] Writing {total_rows} rows across 4 partitions via {PROXY_ENDPOINTS} (job_id={write_job_id}) ===")

    (
        df.write.format("aerospike-flight")
        .mode("append")
        .option("flight.hosts", ",".join(PROXY_ENDPOINTS))
        .option("flight.username", USERNAME)
        .option("flight.password", PASSWORD)
        .option("aerospike.namespace", NAMESPACE)
        .option("aerospike.set", SET_NAME)
        .option("aerospike.keyColumn", "user_id")
        .option("aerospike.writeMode", "update")
        .option("aerospike.ignoreNulls", "false")
        .option("connector.jobId", write_job_id)
        .option("connector.batchSize", "64")
        .option("connector.writerThreads", "2")
        .save()
    )

    # Verify status from both proxy-1 and proxy-2 via pyarrow.flight
    status_node1 = wait_for_job_status("proxy-1:8815", write_job_id, "completed")
    status_node2 = wait_for_job_status("proxy-2:8815", write_job_id, "completed")

    print("  [Raft Status from proxy-1]:", json.dumps(status_node1, indent=2))
    assert status_node1["state"] == "completed", f"Expected completed on proxy-1, got {status_node1}"
    assert status_node2["state"] == "completed", f"Expected completed on proxy-2, got {status_node2}"
    assert status_node1["total_rows_written"] == total_rows, (
        f"Expected {total_rows} rows written, got {status_node1['total_rows_written']}"
    )
    assert status_node2["total_rows_written"] == total_rows, (
        f"Raft replication mismatch on proxy-2: {status_node2['total_rows_written']}"
    )
    per_node = status_node1.get("per_node_rows", {})
    print(f"  [OK] Rows distributed across proxy nodes: {per_node}")
    assert len(per_node) == 2 and sum(per_node.values()) == total_rows, (
        f"Expected both proxy nodes to process rows totaling {total_rows}, got {per_node}"
    )

    # -------------------------------------------------------------------------
    # STEP 3: Positive Data Quality (DQ) Validation (100% Match + NaN == NaN)
    # -------------------------------------------------------------------------
    dq_pass_job_id = "e2e-dq-pass-001"
    print(f"\n=== [Step 3] Running Positive DQ Validation on {total_rows} rows (job_id={dq_pass_job_id}) ===")

    (
        df.write.format("aerospike-flight")
        .mode("append")
        .option("flight.hosts", ",".join(PROXY_ENDPOINTS))
        .option("flight.username", USERNAME)
        .option("flight.password", PASSWORD)
        .option("aerospike.namespace", NAMESPACE)
        .option("aerospike.set", SET_NAME)
        .option("aerospike.keyColumn", "user_id")
        .option("aerospike.dq", "true")
        .option("aerospike.ignoreNulls", "false")
        .option("connector.jobId", dq_pass_job_id)
        .option("connector.batchSize", "64")
        .option("connector.writerThreads", "2")
        .save()
    )

    dq_pass_status = wait_for_job_status("proxy-2:8815", dq_pass_job_id, "completed")
    print("  [DQ Pass Status from proxy-2]:", json.dumps(dq_pass_status, indent=2))
    assert dq_pass_status["is_dq_validation"] is True
    assert dq_pass_status["total_rows_validated"] == total_rows
    assert dq_pass_status["total_matched_rows"] == total_rows
    assert dq_pass_status["total_mismatched_rows"] == 0
    assert dq_pass_status["total_missing_keys"] == 0
    assert dq_pass_status["total_bin_mismatch_rows"] == 0
    assert dq_pass_status["dq_passed"] is True
    print(f"  [OK] Positive DQ Validation passed: {total_rows}/{total_rows} rows matched (including NaN == NaN & nested CDTs)!")

    # -------------------------------------------------------------------------
    # STEP 4: Negative Data Quality (DQ) Validation (Detecting Mismatches & Missing Keys)
    # -------------------------------------------------------------------------
    dq_fail_job_id = "e2e-dq-fail-001"
    print(f"\n=== [Step 4] Running Negative DQ Validation with tampered rows & missing keys (job_id={dq_fail_job_id}) ===")

    subset_rows = list(rows[:8])
    # Modify 3 existing rows (indices 1, 2, 3) to trigger bin_mismatch
    r1 = list(subset_rows[1])
    r1[1] = 999  # change age
    subset_rows[1] = tuple(r1)

    r2 = list(subset_rows[2])
    r2[2] = 999999.99  # change balance
    subset_rows[2] = tuple(r2)

    r3 = list(subset_rows[3])
    r3[6] = ["tampered_tag"]  # change tags array
    subset_rows[3] = tuple(r3)

    # Add 2 non-existent keys to trigger missing_key
    missing1 = list(rows[0])
    missing1[0] = "user_missing_9998"
    missing2 = list(rows[0])
    missing2[0] = "user_missing_9999"
    subset_rows.append(tuple(missing1))
    subset_rows.append(tuple(missing2))

    df_tampered = spark.createDataFrame(subset_rows, schema=schema).repartition(2)

    (
        df_tampered.write.format("aerospike-flight")
        .mode("append")
        .option("flight.hosts", ",".join(PROXY_ENDPOINTS))
        .option("flight.username", USERNAME)
        .option("flight.password", PASSWORD)
        .option("aerospike.namespace", NAMESPACE)
        .option("aerospike.set", SET_NAME)
        .option("aerospike.keyColumn", "user_id")
        .option("aerospike.dq", "true")
        .option("aerospike.ignoreNulls", "false")
        .option("connector.jobId", dq_fail_job_id)
        .save()
    )

    dq_fail_status = wait_for_job_status("proxy-1:8815", dq_fail_job_id, "completed")
    print("  [DQ Fail Status from proxy-1]:", json.dumps(dq_fail_status, indent=2))
    assert dq_fail_status["is_dq_validation"] is True
    assert dq_fail_status["total_rows_validated"] == 10
    assert dq_fail_status["total_matched_rows"] == 5
    assert dq_fail_status["total_mismatched_rows"] == 5
    assert dq_fail_status["total_bin_mismatch_rows"] == 3
    assert dq_fail_status["total_missing_keys"] == 2
    assert dq_fail_status["dq_passed"] is False
    assert len(dq_fail_status["dq_mismatch_samples"]) == 5
    print("  [OK] Negative DQ Validation accurately caught 3 bin mismatches and 2 missing keys!")

    # -------------------------------------------------------------------------
    # STEP 5: Read from Aerospike into Spark DataFrame (Explicit Schema, inferSchema, & PK Pushdown)
    # -------------------------------------------------------------------------
    print(f"\n=== [Step 5] Reading {total_rows} rows back into Spark DataFrame (Partition Scan, inferSchema, & PK Pushdown) ===")

    common_read_opts = {
        "connector.jobId": "e2e-read-job-001",
        "flight.hosts": ",".join(PROXY_ENDPOINTS),
        "flight.username": USERNAME,
        "flight.password": PASSWORD,
        "aerospike.namespace": NAMESPACE,
        "aerospike.set": SET_NAME,
        "aerospike.keyColumn": "user_id",
        "aerospike.read.numPartitions": "8",
    }

    # 5a. Full parallel partition scan with explicit schema (including Date, Timestamp, Array, Map, Struct)
    df_read_full = (
        spark.read.format("aerospike-flight")
        .schema(schema)
        .options(**common_read_opts)
        .load()
    )
    read_count = df_read_full.count()
    assert read_count == total_rows, f"Expected {total_rows} rows from full scan, got {read_count}"

    sample_row = df_read_full.filter("user_id = 'user_0001'").collect()
    assert len(sample_row) == 1
    r_u1 = sample_row[0]
    assert r_u1["age"] == 21
    assert r_u1["is_active"] is False
    assert list(r_u1["tags"]) == ["tag_1", "common"]
    assert r_u1["profile"]["tier"] == "silver"
    assert r_u1["profile"]["points"] == 100
    print(f"  [OK] Full Partition Scan with explicit schema read {read_count} rows (nested CDT Struct/Map/Array verified)!")

    # 5b. Auto schema inference (no .schema(...) call) + Column Pruning
    df_inferred = (
        spark.read.format("aerospike-flight")
        .options(**common_read_opts)
        .option("aerospike.read.sampleSize", "50")
        .load()
    )
    inferred_cols = set(df_inferred.columns)
    assert {"user_id", "age", "balance", "is_active", "tags"}.issubset(inferred_cols), (
        f"Unexpected inferred columns: {inferred_cols}"
    )
    pruned_rows = df_inferred.select("user_id", "age").collect()
    assert len(pruned_rows) == total_rows
    print(f"  [OK] inferSchema + Column Pruning succeeded ({len(inferred_cols)} columns inferred, {len(pruned_rows)} rows read)!")

    # 5c. PK Filter Pushdown (WHERE user_id IN (...)) -> executes BatchGet instead of full scan
    df_pk_pushed = (
        spark.read.format("aerospike-flight")
        .schema(schema)
        .options(**common_read_opts)
        .load()
        .filter("user_id IN ('user_0001', 'user_0010', 'user_0025', 'user_does_not_exist')")
    )
    pushed_rows = df_pk_pushed.collect()
    assert len(pushed_rows) == 3, f"Expected 3 rows from PK pushdown BatchGet, got {len(pushed_rows)}"
    print("  [OK] PK Filter Pushdown (BatchGet) returned 3/3 existing keys!")

    # -------------------------------------------------------------------------
    # STEP 6: Distributed DataFrame Key-Lookup via Arrow Flight DoExchange
    # -------------------------------------------------------------------------
    print("\n=== [Step 6] Enriching Distributed Keys DataFrame via DoExchange (AerospikeFlightLookup & sourceView) ===")
    from pyspark.sql import DataFrame

    keys_data = [
        (1001, "user_0001"),
        (1002, "user_0002"),
        (1003, "user_missing_9999"),
        (1004, "user_0004"),
    ]
    keys_df = spark.createDataFrame(keys_data, ["req_id", "user_id"]).repartition(2)

    # 6a. Direct call via AerospikeFlightLookup.lookup (Left join)
    lookup_opts = dict(common_read_opts)
    lookup_opts["aerospike.lookup.joinType"] = "left"
    jdf_left = spark._jvm.com.asproxy.spark.AerospikeFlightLookup.lookup(
        keys_df._jdf,
        "age INT, is_active BOOLEAN, tags ARRAY<STRING>",
        lookup_opts,
    )
    enriched_left_df = DataFrame(jdf_left, spark)
    left_rows = {r["req_id"]: r for r in enriched_left_df.collect()}
    assert len(left_rows) == 4, f"Expected 4 rows in left lookup, got {len(left_rows)}"
    assert left_rows[1001]["age"] == 21
    assert left_rows[1002]["age"] == 22
    assert left_rows[1003]["age"] is None and left_rows[1003]["tags"] is None
    assert left_rows[1004]["age"] == 24
    print("  [OK] AerospikeFlightLookup.lookup (LEFT join) enriched 4 rows (preserving req_id and setting NULL for missing key)!")

    # 6b. Native spark.read with aerospike.lookup.sourceView (Inner join, no _jvm needed!)
    keys_df.createOrReplaceTempView("keys_to_enrich")
    enriched_inner_df = (
        spark.read.format("aerospike-flight")
        .options(**common_read_opts)
        .option("aerospike.lookup.sourceView", "keys_to_enrich")
        .option("aerospike.lookup.binsSchema", "age INT, is_active BOOLEAN")
        .option("aerospike.lookup.joinType", "inner")
        .load()
    )
    inner_rows = enriched_inner_df.collect()
    assert len(inner_rows) == 3, f"Expected 3 rows in inner lookup via sourceView, got {len(inner_rows)}"
    print("  [OK] spark.read with aerospike.lookup.sourceView (INNER join) returned 3 matched rows without _jvm!")

    print("\n======================================================================")
    print("ALL E2E TESTS PASSED SUCCESSFULLY! (2x Aerospike CE + 2x Proxy + Spark)")
    print("======================================================================\n")
    spark.stop()


if __name__ == "__main__":
    main()
