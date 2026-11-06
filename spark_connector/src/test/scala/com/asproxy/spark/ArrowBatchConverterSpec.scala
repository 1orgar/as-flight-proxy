package com.asproxy.spark

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite

class ArrowBatchConverterSpec extends AnyFunSuite {

  test("AerospikeFlightOptions defaults ignoreNulls to false, includes jobId, and serializes descriptor JSON") {
    val opts = AerospikeFlightOptions.fromMap(
      Map(
        "connector.jobId" -> "job-test-001",
        "flight.hosts" -> "proxy1:8815,proxy2:8815",
        "flight.username" -> "spark_user",
        "flight.password" -> "spark_pass",
        "aerospike.namespace" -> "profile_ns",
        "aerospike.set" -> "features",
        "aerospike.keyColumn" -> "user_id",
        "aerospike.writeMode" -> "update",
        "aerospike.ttl" -> "3600"
      )
    )

    assert(opts.jobId == "job-test-001")
    assert(!opts.ignoreNulls, "ignoreNulls must default to false")
    assert(opts.sendKey)
    assert(opts.hosts.size == 2)
    assert(opts.endpointFor(0, 0) == HostEndpoint("proxy1", 8815))
    assert(opts.endpointFor(0, 1) == HostEndpoint("proxy2", 8815))

    val jsonStr = new String(opts.toFlightDescriptorJsonBytes, "UTF-8")
    assert(jsonStr.contains("\"job_id\":\"job-test-001\""))
    assert(jsonStr.contains("\"namespace\":\"profile_ns\""))
    assert(jsonStr.contains("\"set_name\":\"features\""))
    assert(jsonStr.contains("\"key_column\":\"user_id\""))
    assert(jsonStr.contains("\"write_mode\":\"update\""))
    assert(jsonStr.contains("\"ignore_nulls\":false"))
  }

  test("AerospikeLoadStatus aggregates partition commit messages and stores summary by jobId") {
    AerospikeLoadStatus.clear()
    val opts = AerospikeFlightOptions.fromMap(
      Map(
        "connector.jobId" -> "job-agg-777",
        "aerospike.namespace" -> "ns",
        "aerospike.set" -> "set",
        "aerospike.keyColumn" -> "id"
      )
    )

    val msgs = Seq(
      AerospikeFlightCommitMessage(
        partitionId = 0,
        taskId = 10L,
        rowsWritten = 1000L,
        batchesSent = 2L,
        overloadRetries = 5L,
        otherRetries = 0L,
        elapsedMs = 120L
      ),
      AerospikeFlightCommitMessage(
        partitionId = 1,
        taskId = 11L,
        rowsWritten = 1500L,
        batchesSent = 3L,
        overloadRetries = 2L,
        otherRetries = 1L,
        elapsedMs = 150L
      )
    )

    val summary = AerospikeFlightCommitMessage.aggregate(
      options = opts,
      status = "COMPLETED",
      messages = msgs,
      elapsedMs = 180L,
      raftClusterStatusJson = Some("{\"state\":\"completed\",\"total_rows_written\":2500}")
    )
    AerospikeLoadStatus.recordSummary(summary)

    assert(AerospikeLoadStatus.lastSummary.contains(summary))
    assert(AerospikeLoadStatus.getSummary("job-agg-777").contains(summary))
    assert(summary.rowsWritten == 2500L)
    assert(summary.batchesSent == 5L)
    assert(summary.overloadRetries == 7L)
    assert(summary.otherRetries == 1L)
    assert(summary.partitionsCount == 2)
    assert(summary.toJson.contains("\"rows_written\" : 2500"))
    assert(AerospikeLoadStatus.getSummaryJson("job-agg-777").contains("\"rows_written\" : 2500"))
    assert(AerospikeLoadStatus.getLastSummaryJson().contains("\"job_id\" : \"job-agg-777\""))
  }

  test("ArrowBatchConverter converts primitives, Date, Timestamp, Array, Map, and Struct") {
    val nestedStruct = new StructType()
      .add("city", StringType, nullable = true)
      .add("zip", IntegerType, nullable = true)

    val sparkSchema = new StructType()
      .add("user_id", StringType, nullable = false)
      .add("age", IntegerType, nullable = true)
      .add("score", DoubleType, nullable = true)
      .add("active", BooleanType, nullable = true)
      .add("birth_date", DateType, nullable = true)
      .add("event_ts", TimestampType, nullable = true)
      .add("tags", ArrayType(StringType, containsNull = true), nullable = true)
      .add("metrics", MapType(StringType, LongType, valueContainsNull = false), nullable = true)
      .add("address", nestedStruct, nullable = true)

    val allocator = new RootAllocator(Long.MaxValue)
    val arrowSchema = ArrowSchemaConverter.toArrowSchema(sparkSchema)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)

    try {
      val converter = new ArrowBatchConverter(sparkSchema, "user_id", root)
      converter.reset()

      val row0 = InternalRow(
        UTF8String.fromString("u_100"),
        35,
        98.6,
        true,
        19723, // days since epoch
        1700000000123456L, // micros since epoch
        new GenericArrayData(Array[Any](UTF8String.fromString("gold"), UTF8String.fromString("ml"))),
        new ArrayBasedMapData(
          new GenericArrayData(Array[Any](UTF8String.fromString("views"))),
          new GenericArrayData(Array[Any](150L))
        ),
        InternalRow(UTF8String.fromString("Amsterdam"), 1012)
      )

      val row1 = InternalRow(
        UTF8String.fromString("u_101"),
        null,
        10.0,
        null,
        null,
        null,
        null,
        null,
        null
      )

      converter.appendRow(row0)
      converter.appendRow(row1)
      val count = converter.finishBatch()

      assert(count == 2)
      assert(root.getRowCount == 2)

      val idVec = root.getVector("user_id").asInstanceOf[VarCharVector]
      assert(new String(idVec.get(0), "UTF-8") == "u_100")
      assert(new String(idVec.get(1), "UTF-8") == "u_101")

      val ageVec = root.getVector("age").asInstanceOf[IntVector]
      assert(!ageVec.isNull(0) && ageVec.get(0) == 35)
      assert(ageVec.isNull(1))

      val dateVec = root.getVector("birth_date").asInstanceOf[DateDayVector]
      assert(dateVec.get(0) == 19723)
      assert(dateVec.isNull(1))

      val tsVec = root.getVector("event_ts").asInstanceOf[TimeStampMicroTZVector]
      assert(tsVec.get(0) == 1700000000123456L)
      assert(tsVec.isNull(1))

      val tagsVec = root.getVector("tags").asInstanceOf[ListVector]
      assert(!tagsVec.isNull(0))
      assert(tagsVec.isNull(1))

      val metricsVec = root.getVector("metrics").asInstanceOf[MapVector]
      assert(!metricsVec.isNull(0))
      assert(metricsVec.isNull(1))

      val addrVec = root.getVector("address").asInstanceOf[StructVector]
      assert(!addrVec.isNull(0))
      assert(addrVec.isNull(1))
    } finally {
      root.close()
      allocator.close()
    }
  }

  test("ArrowBatchConverter throws IllegalArgumentException when primary key is null") {
    val sparkSchema = new StructType()
      .add("user_id", StringType, nullable = true)
      .add("age", IntegerType, nullable = true)

    val allocator = new RootAllocator(Long.MaxValue)
    val arrowSchema = ArrowSchemaConverter.toArrowSchema(sparkSchema)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)

    try {
      val converter = new ArrowBatchConverter(sparkSchema, "user_id", root)
      converter.reset()

      val nullKeyRow = InternalRow(null, 25)
      val ex = intercept[IllegalArgumentException] {
        converter.appendRow(nullKeyRow)
      }
      assert(ex.getMessage.contains("Null value encountered in primary key column 'user_id'"))
    } finally {
      root.close()
      allocator.close()
    }
  }
}
