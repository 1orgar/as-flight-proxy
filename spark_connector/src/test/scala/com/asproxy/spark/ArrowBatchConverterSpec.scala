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
    assert(jsonStr.contains("\"validate_only\":false"))

    val dqOpts = AerospikeFlightOptions.fromMap(
      Map(
        "connector.jobId" -> "job-dq-001",
        "aerospike.namespace" -> "profile_ns",
        "aerospike.set" -> "features",
        "aerospike.keyColumn" -> "user_id",
        "aerospike.dq" -> "true"
      )
    )
    assert(dqOpts.validateOnly)
    assert(new String(dqOpts.toFlightDescriptorJsonBytes, "UTF-8").contains("\"validate_only\":true"))
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

  private def defaultTestOptions(extra: Map[String, String] = Map.empty): AerospikeFlightOptions = {
    AerospikeFlightOptions.fromMap(
      Map(
        "connector.jobId" -> "default-test-job",
        "aerospike.namespace" -> "test",
        "aerospike.set" -> "users",
        "aerospike.keyColumn" -> "pk"
      ) ++ extra
    )
  }

  test("ФЛК (AerospikeSchemaValidator): accepts valid schema with all supported scalar and nested CDT types") {
    val validSchema = new StructType()
      .add("pk", StringType, nullable = false)
      .add("b_null", NullType, nullable = true)
      .add("b_bool", BooleanType)
      .add("b_byte", ByteType)
      .add("b_short", ShortType)
      .add("b_int", IntegerType)
      .add("b_long", LongType)
      .add("b_float", FloatType)
      .add("b_double", DoubleType)
      .add("b_dec", DecimalType(18, 4))
      .add("b_str", StringType)
      .add("b_bin", BinaryType)
      .add("b_date", DateType)
      .add("b_ts", TimestampType)
      .add("b_arr", ArrayType(ArrayType(IntegerType)))
      .add("b_map", MapType(StringType, ArrayType(DoubleType)))
      .add(
        "b_struct",
        new StructType()
          .add("nested_id", LongType)
          .add("nested_null", NullType)
          .add("nested_map", MapType(IntegerType, StringType))
      )
      .add("exact_15_bytes!", StringType) // exactly 15 ASCII bytes

    val opts = defaultTestOptions()
    AerospikeSchemaValidator.validate(validSchema, opts)
    // Also verify WriteBuilder driver initialization succeeds
    val wb = new AerospikeFlightWriteBuilder(validSchema, opts)
    assert(wb != null)
  }

  test("ФЛК & ArrowBatchConverter: supports NullType bin columns and rows with PK and all bins null") {
    // 1. Schema where all bin columns are NullType (used to delete multiple bins by PK)
    val allNullTypeBinsSchema = new StructType()
      .add("pk", StringType, nullable = false)
      .add("del_bin_1", NullType, nullable = true)
      .add("del_bin_2", NullType, nullable = true)

    AerospikeSchemaValidator.validate(allNullTypeBinsSchema, defaultTestOptions())

    // 2. Schema with typed bin columns + NullType column where a row has PK and all bins = null
    val mixedSchema = new StructType()
      .add("pk", StringType, nullable = false)
      .add("age", IntegerType, nullable = true)
      .add("name", StringType, nullable = true)
      .add("obsolete_bin", NullType, nullable = true)
      .add("tags", ArrayType(StringType), nullable = true)

    AerospikeSchemaValidator.validate(mixedSchema, defaultTestOptions())

    val allocator = new RootAllocator(Long.MaxValue)
    val arrowSchema = ArrowSchemaConverter.toArrowSchema(mixedSchema)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)

    try {
      val converter = new ArrowBatchConverter(mixedSchema, "pk", root)
      converter.reset()

      // Row with valid PK and ALL bins set to null
      val allBinsNullRow = InternalRow(
        UTF8String.fromString("pk_all_null_bins"),
        null,
        null,
        null,
        null
      )
      converter.appendRow(allBinsNullRow)
      val count = converter.finishBatch()

      assert(count == 1)
      assert(root.getRowCount == 1)
      val pkVec = root.getVector("pk").asInstanceOf[VarCharVector]
      assert(new String(pkVec.get(0), "UTF-8") == "pk_all_null_bins")
      assert(root.getVector("age").isNull(0))
      assert(root.getVector("name").isNull(0))
      assert(root.getVector("obsolete_bin").isNull(0))
      assert(root.getVector("tags").isNull(0))
    } finally {
      root.close()
      allocator.close()
    }

    // 3. When ignoreNulls = true OR writeMode = replace / replace_only / create_only:
    //    - options.shouldOmitNullsInFlight is true
    //    - NullType columns are stripped from the Arrow schema sent to Flight
    //    - Rows where PK is set and all bins are null are skipped on the Spark side (not sent to Flight)
    //    - Schema consisting ONLY of NullType bins is rejected on the Driver
    Seq(
      Map("aerospike.ignoreNulls" -> "true"),
      Map("aerospike.writeMode" -> "replace"),
      Map("aerospike.writeMode" -> "replace_only"),
      Map("aerospike.writeMode" -> "create_only")
    ).foreach { extraOpts =>
      val optsOmit = defaultTestOptions(extraOpts)
      assert(optsOmit.shouldOmitNullsInFlight, s"Expected shouldOmitNullsInFlight=true for $extraOpts")

      val exAllNullIgnored = intercept[IllegalArgumentException] {
        AerospikeSchemaValidator.validate(allNullTypeBinsSchema, optsOmit)
      }
      assert(exAllNullIgnored.getMessage.contains("cannot consist solely of NullType (VOID) bin columns"))
    }

    val replaceOpts = defaultTestOptions(Map("aerospike.writeMode" -> "replace"))
    val allocatorIgnore = new RootAllocator(Long.MaxValue)
    val arrowSchemaIgnore = ArrowSchemaConverter.toArrowSchema(
      sparkSchema = mixedSchema,
      keyColumn = "pk",
      ignoreNulls = replaceOpts.shouldOmitNullsInFlight,
      validateOnly = replaceOpts.validateOnly
    )
    // "obsolete_bin" (NullType) must NOT be in arrowSchemaIgnore
    intercept[IllegalArgumentException] {
      arrowSchemaIgnore.findField("obsolete_bin")
    }
    assert(arrowSchemaIgnore.getFields.size() == 4) // pk, age, name, tags

    val rootIgnore = VectorSchemaRoot.create(arrowSchemaIgnore, allocatorIgnore)
    try {
      val converterIgnore = new ArrowBatchConverter(
        sparkSchema = mixedSchema,
        keyColumn = "pk",
        root = rootIgnore,
        ignoreNulls = replaceOpts.shouldOmitNullsInFlight,
        validateOnly = replaceOpts.validateOnly
      )
      converterIgnore.reset()

      // Row with PK and all bins null -> must be skipped (returns false) and not added to batch!
      val skipped = converterIgnore.appendRow(
        InternalRow(UTF8String.fromString("pk_skipped"), null, null, null, null)
      )
      assert(!skipped)
      assert(converterIgnore.rowCount == 0)

      // Row with at least one non-null bin -> appended (returns true)
      val appended = converterIgnore.appendRow(
        InternalRow(UTF8String.fromString("pk_kept"), 42, null, null, null)
      )
      assert(appended)
      assert(converterIgnore.rowCount == 1)
      assert(converterIgnore.finishBatch() == 1)
    } finally {
      rootIgnore.close()
      allocatorIgnore.close()
    }
  }

  test("ФЛК (AerospikeSchemaValidator): rejects bin names exceeding 15 UTF-8 bytes or custom maxBinNameLen") {
    val schemaTooLongAscii = new StructType()
      .add("pk", StringType, nullable = false)
      .add("bin_name_16bytes", IntegerType) // 16 bytes > 15

    val ex1 = intercept[IllegalArgumentException] {
      new AerospikeFlightWriteBuilder(schemaTooLongAscii, defaultTestOptions())
    }
    assert(ex1.getMessage.contains("Bin name 'bin_name_16bytes' (16 UTF-8 bytes) exceeds Aerospike maximum bin name length (15 bytes)"))

    // Custom stricter maxBinNameLen = 8
    val schema10Bytes = new StructType()
      .add("pk", StringType, nullable = false)
      .add("ten_bytes_", IntegerType) // 10 bytes > 8

    val ex2 = intercept[IllegalArgumentException] {
      new AerospikeFlightWriteBuilder(
        schema10Bytes,
        defaultTestOptions(Map("aerospike.maxBinNameLen" -> "8"))
      )
    }
    assert(ex2.getMessage.contains("exceeds Aerospike maximum bin name length (8 bytes)"))

    // Setting maxBinNameLen > 15 is rejected immediately in options
    val ex3 = intercept[IllegalArgumentException] {
      defaultTestOptions(Map("aerospike.maxBinNameLen" -> "16"))
    }
    assert(ex3.getMessage.contains("'aerospike.maxBinNameLen' must be between 1 and 15 bytes"))
  }

  test("ФЛК (AerospikeSchemaValidator): rejects invalid bin name characters, whitespace, delimiters, non-ASCII, and duplicates") {
    val invalidBinNames = Seq(
      "" -> "cannot be null or empty",
      "   " -> "cannot consist only of whitespace",
      "user age" -> "contains whitespace character",
      "bin\ttab" -> "contains whitespace character",
      "bin\nline" -> "contains whitespace character",
      "bin\u0000null" -> "contains null byte",
      "bin\u0007ctrl" -> "contains control character",
      "bin:colon" -> "contains reserved Aerospike delimiter ':'",
      "bin;semi" -> "contains reserved Aerospike delimiter ';'",
      "бин_кириллица" -> "contains non-ASCII character"
    )

    invalidBinNames.foreach { case (badName, expectedErrorFragment) =>
      val schema = StructType(
        Seq(
          StructField("pk", StringType, nullable = false),
          StructField(badName, IntegerType, nullable = true)
        )
      )
      val ex = intercept[IllegalArgumentException] {
        AerospikeSchemaValidator.validate(schema, defaultTestOptions())
      }
      assert(
        ex.getMessage.contains(expectedErrorFragment),
        s"Expected '$expectedErrorFragment' for bin name '$badName', but got: ${ex.getMessage}"
      )
    }

    // Duplicate bin names
    val duplicateBinSchema = StructType(
      Seq(
        StructField("pk", StringType, nullable = false),
        StructField("score", IntegerType, nullable = true),
        StructField("score", DoubleType, nullable = true)
      )
    )
    val dupEx = intercept[IllegalArgumentException] {
      AerospikeSchemaValidator.validate(duplicateBinSchema, defaultTestOptions())
    }
    assert(dupEx.getMessage.contains("Duplicate bin name 'score'"))
  }

  test("ФЛК (AerospikeSchemaValidator): enforces bin count limits (minimum 1 bin and maximum maxBinsPerRecord)") {
    // 0 bin columns (only PK column)
    val onlyPkSchema = new StructType().add("pk", StringType, nullable = false)
    val exZeroBins = intercept[IllegalArgumentException] {
      AerospikeSchemaValidator.validate(onlyPkSchema, defaultTestOptions())
    }
    assert(exZeroBins.getMessage.contains("DataFrame must contain at least 1 bin column"))

    // Exceeding maxBinsPerRecord
    val fourBinsSchema = new StructType()
      .add("pk", StringType, nullable = false)
      .add("b1", IntegerType)
      .add("b2", IntegerType)
      .add("b3", IntegerType)
      .add("b4", IntegerType)

    val exTooManyBins = intercept[IllegalArgumentException] {
      AerospikeSchemaValidator.validate(
        fourBinsSchema,
        defaultTestOptions(Map("aerospike.maxBinsPerRecord" -> "3"))
      )
    }
    assert(exTooManyBins.getMessage.contains("DataFrame contains 4 bin columns, which exceeds the maximum allowed bins per record (3)"))
  }

  test("ФЛК (AerospikeSchemaValidator): validates primary key column existence and allowed primary key data types") {
    // Missing PK column
    val missingPkSchema = new StructType()
      .add("other_id", StringType)
      .add("age", IntegerType)
    val exMissing = intercept[IllegalArgumentException] {
      AerospikeSchemaValidator.validate(missingPkSchema, defaultTestOptions())
    }
    assert(exMissing.getMessage.contains("Primary key column 'pk' not found in DataFrame schema"))

    // Allowed PK types
    Seq(StringType, ByteType, ShortType, IntegerType, LongType, BinaryType).foreach { validPkType =>
      val s = new StructType()
        .add("pk", validPkType, nullable = false)
        .add("val", IntegerType)
      AerospikeSchemaValidator.validate(s, defaultTestOptions())
    }

    // Disallowed PK types (NullType, Double, Float, Boolean, Date, Timestamp, Decimal, Array, Map, Struct)
    val invalidPkTypes: Seq[DataType] = Seq(
      NullType,
      DoubleType,
      FloatType,
      BooleanType,
      DateType,
      TimestampType,
      DecimalType(10, 2),
      ArrayType(StringType),
      MapType(StringType, IntegerType),
      new StructType().add("a", IntegerType)
    )

    invalidPkTypes.foreach { badPkType =>
      val s = new StructType()
        .add("pk", badPkType, nullable = false)
        .add("val", IntegerType)
      val ex = intercept[IllegalArgumentException] {
        AerospikeSchemaValidator.validate(s, defaultTestOptions())
      }
      assert(
        ex.getMessage.contains("Primary key column 'pk' has unsupported data type"),
        s"Expected PK type rejection for $badPkType, got: ${ex.getMessage}"
      )
    }
  }

  test("ФЛК (AerospikeSchemaValidator): validates top-level and deeply nested data types in schema") {
    // 1. Top-level unsupported types (note: NullType IS supported for bin deletion)
    val unsupportedTopLevel: Seq[DataType] = Seq(
      CalendarIntervalType,
      DayTimeIntervalType(),
      YearMonthIntervalType(),
      TimestampNTZType,
      CharType(10),
      VarcharType(20)
    )

    unsupportedTopLevel.foreach { badType =>
      val s = new StructType()
        .add("pk", StringType)
        .add("bad_col", badType)
      val ex = intercept[IllegalArgumentException] {
        AerospikeSchemaValidator.validate(s, defaultTestOptions())
      }
      assert(ex.getMessage.contains("bin 'bad_col'"))
    }

    // 2. Unsupported type inside nested Array
    val badArraySchema = new StructType()
      .add("pk", StringType)
      .add("arr_col", ArrayType(CalendarIntervalType))
    val exArr = intercept[IllegalArgumentException] {
      AerospikeSchemaValidator.validate(badArraySchema, defaultTestOptions())
    }
    assert(exArr.getMessage.contains("bin 'arr_col'[]"))

    // 3. Unsupported Map key type (DoubleType and ArrayType are not valid Aerospike Map keys)
    val badMapKeySchema = new StructType()
      .add("pk", StringType)
      .add("map_col", MapType(DoubleType, StringType))
    val exMapKey = intercept[IllegalArgumentException] {
      AerospikeSchemaValidator.validate(badMapKeySchema, defaultTestOptions())
    }
    assert(exMapKey.getMessage.contains("Unsupported Map key type 'double' at bin 'map_col'{key}"))

    // 4. Unsupported type inside deeply nested Struct -> Array -> Map value
    val deepBadSchema = new StructType()
      .add("pk", StringType)
      .add(
        "profile",
        new StructType()
          .add("events", ArrayType(MapType(StringType, TimestampNTZType)))
      )
    val exDeep = intercept[IllegalArgumentException] {
      AerospikeSchemaValidator.validate(deepBadSchema, defaultTestOptions())
    }
    assert(exDeep.getMessage.contains("bin 'profile'.events[]{value}"))

    // 5. Empty nested StructType
    val emptyStructSchema = new StructType()
      .add("pk", StringType)
      .add("empty_s", new StructType())
    val exEmptyStruct = intercept[IllegalArgumentException] {
      AerospikeSchemaValidator.validate(emptyStructSchema, defaultTestOptions())
    }
    assert(exEmptyStruct.getMessage.contains("Empty StructType (0 fields) is not allowed at bin 'empty_s'"))
  }

  test("ФЛК (AerospikeSchemaValidator): aggregates multiple schema and bin errors in a single exception") {
    val multiErrorSchema = new StructType()
      .add("pk", DoubleType) // [1] invalid PK type
      .add("too_long_bin_name_123", IntegerType) // [2] > 15 bytes
      .add("bad space", StringType) // [3] whitespace in bin name
      .add("bad_type", CalendarIntervalType) // [4] unsupported interval type

    val ex = intercept[IllegalArgumentException] {
      AerospikeSchemaValidator.validate(multiErrorSchema, defaultTestOptions())
    }
    assert(ex.getMessage.contains("failed with 4 error(s)"))
    assert(ex.getMessage.contains("[1]"))
    assert(ex.getMessage.contains("[2]"))
    assert(ex.getMessage.contains("[3]"))
    assert(ex.getMessage.contains("[4]"))
  }

  test("ФЛК (AerospikeSchemaValidator): validates namespace and set name length and characters") {
    val validRowSchema = new StructType()
      .add("pk", StringType)
      .add("v", IntegerType)

    // Namespace > 31 bytes
    val longNs = "ns_" + ("a" * 30)
    val exNsLen = intercept[IllegalArgumentException] {
      AerospikeSchemaValidator.validate(validRowSchema, defaultTestOptions(Map("aerospike.namespace" -> longNs)))
    }
    assert(exNsLen.getMessage.contains("exceeds Aerospike limit of 31 bytes"))

    // Set > 63 bytes
    val longSet = "set_" + ("b" * 62)
    val exSetLen = intercept[IllegalArgumentException] {
      AerospikeSchemaValidator.validate(validRowSchema, defaultTestOptions(Map("aerospike.set" -> longSet)))
    }
    assert(exSetLen.getMessage.contains("exceeds Aerospike limit of 63 bytes"))

    // Invalid delimiter ':' in set name
    val exSetChar = intercept[IllegalArgumentException] {
      AerospikeSchemaValidator.validate(validRowSchema, defaultTestOptions(Map("aerospike.set" -> "bad:set")))
    }
    assert(exSetChar.getMessage.contains("Set name 'bad:set' contains invalid character"))
  }

  test("ArrowSchemaConverter & ArrowRowReader: round-trip Spark StructType <-> Arrow Schema and InternalRow <-> VectorSchemaRoot") {
    val nestedStruct = new StructType()
      .add("city", StringType, nullable = true)
      .add("zip", IntegerType, nullable = true)

    val sparkSchema = new StructType()
      .add("user_id", StringType, nullable = false)
      .add("age", IntegerType, nullable = true)
      .add("score", DoubleType, nullable = true)
      .add("active", BooleanType, nullable = true)
      .add("amount", DecimalType(12, 2), nullable = true)
      .add("birth_date", DateType, nullable = true)
      .add("event_ts", TimestampType, nullable = true)
      .add("tags", ArrayType(StringType, containsNull = true), nullable = true)
      .add("metrics", MapType(StringType, LongType, valueContainsNull = false), nullable = true)
      .add("address", nestedStruct, nullable = true)

    val arrowSchema = ArrowSchemaConverter.toArrowSchema(sparkSchema)
    val roundTripSchema = ArrowSchemaConverter.fromArrowSchema(arrowSchema)
    assert(roundTripSchema == sparkSchema)

    val b64Ipc = ArrowSchemaConverter.toArrowSchemaIpcBase64(sparkSchema)
    assert(b64Ipc != null && b64Ipc.nonEmpty)

    val allocator = new RootAllocator(Long.MaxValue)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    try {
      val converter = new ArrowBatchConverter(sparkSchema, "user_id", root)
      converter.reset()

      val row0 = InternalRow(
        UTF8String.fromString("u_100"),
        35,
        98.6,
        true,
        Decimal(BigDecimal("1234.56"), 12, 2),
        19723,
        1700000000123456L,
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
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
      )

      converter.appendRow(row0)
      converter.appendRow(row1)
      converter.finishBatch()

      val reader = new ArrowRowReader(sparkSchema, root)
      val out0 = reader.readRow(0)
      assert(out0.getUTF8String(0).toString == "u_100")
      assert(out0.getInt(1) == 35)
      assert(out0.getDouble(2) == 98.6)
      assert(out0.getBoolean(3))
      assert(out0.getDecimal(4, 12, 2) == Decimal(BigDecimal("1234.56"), 12, 2))
      assert(out0.getInt(5) == 19723)
      assert(out0.getLong(6) == 1700000000123456L)
      assert(out0.getArray(7).numElements() == 2)
      assert(out0.getArray(7).getUTF8String(0).toString == "gold")
      assert(out0.getMap(8).numElements() == 1)
      assert(out0.getMap(8).valueArray().getLong(0) == 150L)
      assert(out0.getStruct(9, 2).getUTF8String(0).toString == "Amsterdam")
      assert(out0.getStruct(9, 2).getInt(1) == 1012)

      val out1 = reader.readRow(1)
      assert(out1.getUTF8String(0).toString == "u_101")
      for (colIdx <- 1 until sparkSchema.fields.length) {
        assert(out1.isNullAt(colIdx), s"Expected null at colIdx=$colIdx")
      }
    } finally {
      root.close()
      allocator.close()
    }
  }

  test("AerospikeFlightScanBuilder: splits 4096 partitions evenly, prunes columns, and pushes down PK filters to BatchGet") {
    import org.apache.spark.sql.sources.{EqualTo, GreaterThan, In, Or}

    val fullSchema = new StructType()
      .add("pk", StringType, nullable = false)
      .add("age", IntegerType, nullable = true)
      .add("score", DoubleType, nullable = true)

    // 1. Default PartitionRangeScan across 16 splits covers all 4096 partitions
    val opts16 = defaultTestOptions(Map("aerospike.read.numPartitions" -> "16"))
    val sb1 = new AerospikeFlightScanBuilder(fullSchema, opts16)
    val pruned = new StructType().add("pk", StringType, nullable = false).add("age", IntegerType, nullable = true)
    sb1.pruneColumns(pruned)
    val scan1 = sb1.build().asInstanceOf[AerospikeFlightScan]
    assert(scan1.readSchema == pruned)

    val parts1 = scan1.planInputPartitions().map(_.asInstanceOf[AerospikePartitionRangeInputPartition])
    assert(parts1.length == 16)
    assert(parts1.map(_.partitionCount).sum == 4096)
    assert(parts1.head.beginPartition == 0)
    assert(parts1.last.beginPartition + parts1.last.partitionCount == 4096)

    // 2. PK filter pushdown (IN + non-PK filter) converts scan to AerospikeBatchKeysInputPartition
    val sb2 = new AerospikeFlightScanBuilder(fullSchema, defaultTestOptions(Map("aerospike.read.numPartitions" -> "2")))
    val remaining = sb2.pushFilters(
      Array(
        In("pk", Array[Any]("u_1", "u_2", "u_3")),
        GreaterThan("age", 20)
      )
    )
    assert(remaining.length == 1) // GreaterThan("age", 20) stays as post-scan filter
    assert(sb2.pushedFilters().length == 1)

    val scan2 = sb2.build().asInstanceOf[AerospikeFlightScan]
    val parts2 = scan2.planInputPartitions().map(_.asInstanceOf[AerospikeBatchKeysInputPartition])
    assert(parts2.length == 2)
    assert(parts2.flatMap(_.keys).length == 3)

    // 3. OR of EqualTo on PK is also pushed down
    val sb3 = new AerospikeFlightScanBuilder(fullSchema, defaultTestOptions())
    sb3.pushFilters(Array(Or(EqualTo("pk", "k_a"), EqualTo("pk", "k_b"))))
    val scan3 = sb3.build().asInstanceOf[AerospikeFlightScan]
    val parts3 = scan3.planInputPartitions().map(_.asInstanceOf[AerospikeBatchKeysInputPartition])
    assert(parts3.flatMap(_.keys).toSeq == Seq(KeyLiteralSpec.StringKey("k_a"), KeyLiteralSpec.StringKey("k_b")))
  }

  test("AerospikeFlightLookup: parses DDL/JSON binsSchema and builds output schema for left and inner joins") {
    val inputSchema = new StructType()
      .add("req_id", LongType, nullable = false)
      .add("pk", StringType, nullable = false)

    val ddlBins = AerospikeFlightLookup.parseBinsSchema("age INT, score DOUBLE, tags ARRAY<STRING>")
    assert(ddlBins.fields.length == 3)

    val jsonBins = AerospikeFlightLookup.parseBinsSchema(ddlBins.json)
    assert(jsonBins == ddlBins)

    val leftOut = AerospikeFlightLookup.buildOutputSchema(inputSchema, ddlBins, "pk", "left")
    assert(leftOut.fieldNames.toSeq == Seq("req_id", "pk", "age", "score", "tags"))
    assert(leftOut("age").nullable)

    val innerOut = AerospikeFlightLookup.buildOutputSchema(
      inputSchema,
      new StructType().add("pk", StringType, nullable = false).add("age", IntegerType, nullable = false),
      "pk",
      "inner"
    )
    assert(innerOut.fieldNames.toSeq == Seq("req_id", "pk", "age"))
    assert(!innerOut("age").nullable)
  }

  test("AerospikeFlightOptions, KeyLiteralSpec, and AerospikeCustomMetrics cover all option aliases and validations") {
    import org.apache.spark.sql.util.CaseInsensitiveStringMap
    import scala.collection.JavaConverters._

    val raw = Map(
      "aerospike.job_id" -> "job-alias-1",
      "aerospike.flight.endpoint" -> "127.0.0.1:8815",
      "aerospike.flight.username" -> "u1",
      "aerospike.flight.password" -> "p1",
      "aerospike.flight.tls.enabled" -> "false",
      "aerospike.flight.tls.ca_cert_path" -> "/tmp/ca.pem",
      "aerospike.flight.tls.client_cert_path" -> "/tmp/cert.pem",
      "aerospike.flight.tls.client_key_path" -> "/tmp/key.pem",
      "aerospike.flight.tls.verify_server" -> "false",
      "aerospike.flight.timeout_ms" -> "15000",
      "aerospike.namespace" -> "test",
      "aerospike.set" -> "users",
      "aerospike.key_column" -> "pk",
      "aerospike.write_mode" -> "dq",
      "aerospike.ttl_seconds" -> "120",
      "aerospike.send_key" -> "false",
      "aerospike.ignore_nulls" -> "true",
      "aerospike.use_batch_write" -> "true",
      "aerospike.batch_size" -> "256",
      "aerospike.writer_threads" -> "2",
      "aerospike.queue_capacity" -> "4",
      "aerospike.read.num_partitions" -> "8",
      "aerospike.read.batch_size" -> "512",
      "aerospike.read.sample_size" -> "50",
      "aerospike.read.type_mismatch_mode" -> "null",
      "aerospike.read.keys" -> "k1,k2",
      "aerospike.lookup.source_view" -> "my_view",
      "aerospike.lookup.join_type" -> "inner"
    )

    val opts = AerospikeFlightOptions.fromSparkOptions(new CaseInsensitiveStringMap(raw.asJava))
    assert(opts.jobId == "job-alias-1")
    assert(opts.hosts.head.toString == "127.0.0.1:8815")
    assert(opts.username == "u1" && opts.password == "p1")
    assert(!opts.tlsVerifyServer)
    assert(opts.tlsCaCertPath.contains("/tmp/ca.pem"))
    assert(opts.tlsClientCertPath.contains("/tmp/cert.pem"))
    assert(opts.tlsClientKeyPath.contains("/tmp/key.pem"))
    assert(opts.validateOnly)
    assert(opts.shouldOmitNullsInFlight)
    assert(opts.readTypeMismatchMode == "null")
    assert(opts.lookupJoinType == "inner")
    assert(!opts.autoConvertStrings)
    val defaultDescJson = new String(opts.toFlightDescriptorJsonBytes, "UTF-8")
    assert(defaultDescJson.contains("\"auto_convert_strings\":false"))

    val autoConvertOpts = AerospikeFlightOptions.fromMap(raw + ("connector.autoConvertStrings" -> "true"))
    assert(autoConvertOpts.autoConvertStrings)
    val autoConvertDescJson = new String(autoConvertOpts.toFlightDescriptorJsonBytes, "UTF-8")
    assert(autoConvertDescJson.contains("\"auto_convert_strings\":true"))
    opts.validateWriteTarget()

    val statusOpts = AerospikeFlightOptions.forStatusQuery(Map("flight.hosts" -> "localhost:8815"))
    assert(statusOpts.namespace.isEmpty)

    intercept[IllegalArgumentException] {
      AerospikeFlightOptions.fromMap(raw - "aerospike.job_id")
    }
    intercept[IllegalArgumentException] {
      AerospikeFlightOptions.fromMap(raw.updated("aerospike.write_mode", "bad_mode"))
    }
    intercept[IllegalArgumentException] {
      AerospikeFlightOptions.fromMap(raw.updated("aerospike.read.type_mismatch_mode", "bad_mismatch"))
    }
    intercept[IllegalArgumentException] {
      AerospikeFlightOptions.fromMap(raw.updated("aerospike.lookup.join_type", "full_outer"))
    }
    intercept[IllegalArgumentException] {
      AerospikeFlightOptions.fromMap(raw.updated("flight.hosts", "invalid_host_without_port"))
    }

    // KeyLiteralSpec tests
    assert(KeyLiteralSpec.StringKey("s").toMap.get("type") == "string")
    assert(KeyLiteralSpec.IntKey(42L).toMap.get("type") == "int")
    assert(KeyLiteralSpec.BytesKey(Array[Byte](1, 2, -1)).toMap.get("type") == "bytes")

    assert(KeyLiteralSpec.fromSparkValue(null, StringType).isEmpty)
    assert(KeyLiteralSpec.fromSparkValue(10, IntegerType).contains(KeyLiteralSpec.IntKey(10L)))
    assert(KeyLiteralSpec.fromSparkValue("20", LongType).contains(KeyLiteralSpec.IntKey(20L)))
    assert(KeyLiteralSpec.fromSparkValue("not_a_num", LongType).isEmpty)
    assert(KeyLiteralSpec.fromSparkValue(Array[Byte](1, 2), BinaryType).isDefined)
    assert(KeyLiteralSpec.fromSparkValue(java.util.Base64.getEncoder.encodeToString(Array[Byte](3, 4)), BinaryType).isDefined)
    assert(KeyLiteralSpec.fromSparkValue(12345, BinaryType).isEmpty)

    assert(KeyLiteralSpec.parseOptionKeys("", StringType).isEmpty)
    assert(
      KeyLiteralSpec.parseOptionKeys("[10, \"20\"]", LongType) ==
        Seq(KeyLiteralSpec.IntKey(10L), KeyLiteralSpec.IntKey(20L))
    )
    assert(
      KeyLiteralSpec.parseOptionKeys("a, b", StringType) ==
        Seq(KeyLiteralSpec.StringKey("a"), KeyLiteralSpec.StringKey("b"))
    )

    // CustomMetrics descriptions & names
    val allMetrics = AerospikeCustomMetrics.supportedMetrics ++ AerospikeCustomMetrics.supportedReadMetrics
    assert(allMetrics.length == 9)
    allMetrics.foreach { m =>
      assert(m.name().startsWith("as_"))
      assert(m.description().nonEmpty)
    }
  }

  test("ArrowSchemaConverter, ArrowBatchConverter, and ArrowRowReader cover Byte, Short, Float, Binary, Null, and coercions") {
    import org.apache.arrow.vector.types.TimeUnit
    import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
    import java.util.Collections

    val schema = new StructType()
      .add("pk", LongType, nullable = false)
      .add("b_byte", ByteType, nullable = true)
      .add("b_short", ShortType, nullable = true)
      .add("b_float", FloatType, nullable = true)
      .add("b_bin", BinaryType, nullable = true)
      .add("b_null", NullType, nullable = true)

    val arrowSchema = ArrowSchemaConverter.toArrowSchema(schema)
    assert(ArrowSchemaConverter.fromArrowSchema(arrowSchema) == schema)

    val allocator = new RootAllocator(Long.MaxValue)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    try {
      // Missing keyColumn throws
      intercept[IllegalArgumentException] {
        new ArrowBatchConverter(schema, "missing_pk", root)
      }

      val conv = new ArrowBatchConverter(schema, "pk", root)
      conv.reset()
      conv.appendRow(InternalRow(1L, 7.toByte, 300.toShort, 1.5f, Array[Byte](9, 8, 7), null))
      conv.appendRow(InternalRow(2L, null, null, null, null, null))
      conv.finishBatch()

      val reader = new ArrowRowReader(schema, root)
      val r0 = reader.readRow(0)
      assert(r0.getLong(0) == 1L)
      assert(r0.getByte(1) == 7.toByte)
      assert(r0.getShort(2) == 300.toShort)
      assert(r0.getFloat(3) == 1.5f)
      assert(r0.getBinary(4).toSeq == Seq[Byte](9, 8, 7))
      assert(r0.isNullAt(5))

      val r1 = reader.readRow(1)
      assert(r1.getLong(0) == 2L)
      assert(r1.isNullAt(1) && r1.isNullAt(2) && r1.isNullAt(3) && r1.isNullAt(4) && r1.isNullAt(5))

      // Coercion checks in ArrowRowReader:
      // Int <-> Long, Float <-> Double, TimestampMilli/Micro
      val coerceArrowSchema = new Schema(
        java.util.Arrays.asList(
          new Field("i_from_long", FieldType.nullable(new ArrowType.Int(64, true)), Collections.emptyList()),
          new Field("l_from_int", FieldType.nullable(new ArrowType.Int(32, true)), Collections.emptyList()),
          new Field(
            "f_from_double",
            FieldType.nullable(new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)),
            Collections.emptyList()
          ),
          new Field(
            "d_from_float",
            FieldType.nullable(new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE)),
            Collections.emptyList()
          ),
          new Field("ts_ms", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")), Collections.emptyList()),
          new Field("ts_ms_ntz", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), Collections.emptyList()),
          new Field("ts_us_ntz", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)), Collections.emptyList())
        )
      )
      val coerceRoot = VectorSchemaRoot.create(coerceArrowSchema, allocator)
      try {
        coerceRoot.allocateNew()
        coerceRoot.getVector("i_from_long").asInstanceOf[BigIntVector].setSafe(0, 42L)
        coerceRoot.getVector("l_from_int").asInstanceOf[IntVector].setSafe(0, 99)
        coerceRoot.getVector("f_from_double").asInstanceOf[Float8Vector].setSafe(0, 2.5)
        coerceRoot.getVector("d_from_float").asInstanceOf[Float4Vector].setSafe(0, 3.5f)
        coerceRoot.getVector("ts_ms").asInstanceOf[TimeStampMilliTZVector].setSafe(0, 1000L)
        coerceRoot.getVector("ts_ms_ntz").asInstanceOf[TimeStampMilliVector].setSafe(0, 2000L)
        coerceRoot.getVector("ts_us_ntz").asInstanceOf[TimeStampMicroVector].setSafe(0, 3000000L)
        coerceRoot.setRowCount(1)

        val targetSparkSchema = new StructType()
          .add("i_from_long", IntegerType)
          .add("l_from_int", LongType)
          .add("f_from_double", FloatType)
          .add("d_from_float", DoubleType)
          .add("ts_ms", TimestampType)
          .add("ts_ms_ntz", TimestampType)
          .add("ts_us_ntz", TimestampType)
          .add("missing_vec", StringType)

        val cr = new ArrowRowReader(targetSparkSchema, coerceRoot).readRow(0)
        assert(cr.getInt(0) == 42)
        assert(cr.getLong(1) == 99L)
        assert(cr.getFloat(2) == 2.5f)
        assert(cr.getDouble(3) == 3.5)
        assert(cr.getLong(4) == 1000000L)
        assert(cr.getLong(5) == 2000000L)
        assert(cr.getLong(6) == 3000000L)
        assert(cr.isNullAt(7))
      } finally {
        coerceRoot.close()
      }
    } finally {
      root.close()
      allocator.close()
    }
  }

  test("End-to-End Spark DataSource V2 Write, Read, Filter Pushdown, Schema Inference, Lookup, and Status with Embedded FlightServer") {
    import com.fasterxml.jackson.databind.ObjectMapper
    import org.apache.arrow.flight._
    import org.apache.arrow.flight.auth.ServerAuthHandler
    import org.apache.arrow.vector.ipc.ReadChannel
    import org.apache.arrow.vector.ipc.message.MessageSerializer
    import org.apache.spark.sql.{Row, SparkSession}
    import org.apache.spark.sql.connector.write.PhysicalWriteInfo
    import java.io.ByteArrayInputStream
    import java.nio.channels.Channels
    import java.nio.charset.StandardCharsets
    import java.util.{Base64, Optional}
    import java.util.concurrent.ConcurrentHashMap
    import scala.collection.JavaConverters._

    val allocator = new RootAllocator(Long.MaxValue)
    val mapper = new ObjectMapper()

    // In-memory store: pk -> Map(binName -> Any)
    val store = new ConcurrentHashMap[String, Map[String, Any]]()

    val producer = new NoOpFlightProducer {
      override def acceptPut(
          context: FlightProducer.CallContext,
          flightStream: FlightStream,
          ackStream: FlightProducer.StreamListener[PutResult]
      ): Runnable = {
        FlightCompressionHelper.enableStreamDecompression(flightStream)
        new Runnable {
          override def run(): Unit = {
            try {
              val descBytes = flightStream.getDescriptor.getCommand
              val descNode = mapper.readTree(descBytes)
              val keyCol = descNode.get("key_column").asText("pk")
              val validateOnly = descNode.has("validate_only") && descNode.get("validate_only").asBoolean(false)

              var batchIdx = 0
              while (flightStream.next()) {
                val root = flightStream.getRoot
                val sparkSchema = ArrowSchemaConverter.fromArrowSchema(root.getSchema)
                val rowReader = new ArrowRowReader(sparkSchema, root)
                val pkIdx = sparkSchema.fieldIndex(keyCol)
                val rowCount = root.getRowCount

                var r = 0
                while (r < rowCount) {
                  val row = rowReader.readRow(r)
                  val pkStr = row.getUTF8String(pkIdx).toString
                  if (!validateOnly) {
                    val bins = sparkSchema.fields.zipWithIndex.flatMap { case (f, idx) =>
                      if (idx == pkIdx || row.isNullAt(idx)) None
                      else {
                        val v: Any = f.dataType match {
                          case IntegerType => row.getInt(idx)
                          case LongType    => row.getLong(idx)
                          case DoubleType  => row.getDouble(idx)
                          case StringType  => row.getUTF8String(idx).toString
                          case BooleanType => row.getBoolean(idx)
                          case _           => row.get(idx, f.dataType)
                        }
                        Some(f.name -> v)
                      }
                    }.toMap
                    store.put(pkStr, bins)
                  }
                  r += 1
                }

                val ackMap = new java.util.LinkedHashMap[String, Any]()
                ackMap.put("batch_index", Int.box(batchIdx))
                ackMap.put("rows_written", Int.box(if (validateOnly) 0 else rowCount))
                ackMap.put("overload_retries", Int.box(1))
                ackMap.put("other_retries", Int.box(0))
                ackMap.put("rows_validated", Int.box(if (validateOnly) rowCount else 0))
                ackMap.put("matched_rows", Int.box(if (validateOnly) rowCount else 0))
                ackMap.put("mismatched_rows", Int.box(0))
                ackMap.put("missing_keys", Int.box(0))
                ackMap.put("bin_mismatch_rows", Int.box(0))
                val ackBytes = mapper.writeValueAsBytes(ackMap)

                val ackBuf = allocator.buffer(ackBytes.length)
                ackBuf.writeBytes(ackBytes)
                val putRes = PutResult.metadata(ackBuf)
                ackStream.onNext(putRes)
                putRes.close()
                batchIdx += 1
              }
              ackStream.onCompleted()
            } catch {
              case t: Throwable =>
                ackStream.onError(t)
            }
          }
        }
      }

      override def getSchema(
          context: FlightProducer.CallContext,
          descriptor: FlightDescriptor
      ): SchemaResult = {
        val inferredSpark = new StructType()
          .add("pk", StringType, nullable = false)
          .add("age", IntegerType, nullable = true)
          .add("score", DoubleType, nullable = true)
        new SchemaResult(ArrowSchemaConverter.toArrowSchema(inferredSpark))
      }

      override def getStream(
          context: FlightProducer.CallContext,
          ticket: Ticket,
          listener: FlightProducer.ServerStreamListener
      ): Unit = {
        val node = mapper.readTree(ticket.getBytes)
        val keyCol = node.get("key_column").asText("pk")
        val schemaB64 = node.get("schema_ipc_base64").asText()
        val schemaBytes = Base64.getDecoder.decode(schemaB64)
        val arrowSchema = MessageSerializer.deserializeSchema(
          new ReadChannel(Channels.newChannel(new ByteArrayInputStream(schemaBytes)))
        )
        val wireSparkSchema = ArrowSchemaConverter.fromArrowSchema(arrowSchema)

        val readModeNode = node.get("read_mode")
        val mode = readModeNode.get("mode").asText()
        val matchingEntries: Seq[(String, Map[String, Any])] = if (mode == "partition_range") {
          val begin = readModeNode.get("begin_partition").asInt()
          if (begin == 0) {
            store.entrySet().asScala.toSeq.sortBy(_.getKey).map(e => e.getKey -> e.getValue)
          } else {
            Seq.empty
          }
        } else {
          val keysArr = readModeNode.get("keys")
          val reqKeys = keysArr.elements().asScala.map(_.get("value").asText()).toSeq
          reqKeys.flatMap(k => Option(store.get(k)).map(v => k -> v))
        }

        val root = VectorSchemaRoot.create(arrowSchema, allocator)
        try {
          val comp = if (node.has("compression")) node.get("compression").asText("zstd") else "zstd"
          listener.start(root)
          FlightCompressionHelper.configureOutboundStream(listener, root, comp)
          val conv = new ArrowBatchConverter(wireSparkSchema, keyCol, root, ignoreNulls = false, validateOnly = true)
          conv.reset()
          matchingEntries.foreach { case (pk, bins) =>
            val values = wireSparkSchema.fields.map { f =>
              if (f.name == keyCol) UTF8String.fromString(pk)
              else {
                bins.get(f.name) match {
                  case Some(s: String) => UTF8String.fromString(s)
                  case Some(v)         => v
                  case None            => null
                }
              }
            }
            conv.appendRow(InternalRow.fromSeq(values))
          }
          conv.finishBatch()
          listener.putNext()
          listener.completed()
        } finally {
          root.close()
        }
      }

      override def doExchange(
          context: FlightProducer.CallContext,
          reader: FlightStream,
          writer: FlightProducer.ServerStreamListener
      ): Unit = {
        FlightCompressionHelper.enableStreamDecompression(reader)
        val descBytes = reader.getDescriptor.getCommand
        val node = mapper.readTree(descBytes)
        val keyCol = node.get("key_column").asText("pk")
        val joinType = node.get("join_type").asText("left")
        val comp = if (node.has("compression")) node.get("compression").asText("zstd") else "zstd"
        val binsB64 = node.get("bins_schema_ipc_base64").asText()
        val binsArrowSchema = MessageSerializer.deserializeSchema(
          new ReadChannel(Channels.newChannel(new ByteArrayInputStream(Base64.getDecoder.decode(binsB64))))
        )
        val binsSparkSchema = ArrowSchemaConverter.fromArrowSchema(binsArrowSchema)

        var outRoot: VectorSchemaRoot = null
        var outSparkSchema: StructType = null
        var outConv: ArrowBatchConverter = null

        try {
          while (reader.next()) {
            val inRoot = reader.getRoot
            val inSparkSchema = ArrowSchemaConverter.fromArrowSchema(inRoot.getSchema)
            val inReader = new ArrowRowReader(inSparkSchema, inRoot)
            val pkIdx = inSparkSchema.fieldIndex(keyCol)

            if (outRoot == null) {
              outSparkSchema = AerospikeFlightLookup.buildOutputSchema(inSparkSchema, binsSparkSchema, keyCol, joinType)
              outRoot = VectorSchemaRoot.create(ArrowSchemaConverter.toArrowSchema(outSparkSchema), allocator)
              writer.start(outRoot)
              FlightCompressionHelper.configureOutboundStream(writer, outRoot, comp)
              outConv = new ArrowBatchConverter(outSparkSchema, keyCol, outRoot, ignoreNulls = false, validateOnly = true)
            }

            outConv.reset()
            var r = 0
            while (r < inRoot.getRowCount) {
              val inRow = inReader.readRow(r)
              val pkStr = inRow.getUTF8String(pkIdx).toString
              val foundBinsOpt = Option(store.get(pkStr))
              if (foundBinsOpt.isDefined || joinType == "left") {
                val binsMap = foundBinsOpt.getOrElse(Map.empty)
                val outVals = outSparkSchema.fields.map { f =>
                  val inFieldIdx = inSparkSchema.fields.indexWhere(_.name == f.name)
                  if (inFieldIdx >= 0) {
                    inRow.get(inFieldIdx, f.dataType)
                  } else {
                    binsMap.get(f.name) match {
                      case Some(s: String) => UTF8String.fromString(s)
                      case Some(v)         => v
                      case None            => null
                    }
                  }
                }
                outConv.appendRow(InternalRow.fromSeq(outVals))
              }
              r += 1
            }
            outConv.finishBatch()
            writer.putNext()
          }
          writer.completed()
        } finally {
          if (outRoot != null) outRoot.close()
        }
      }

      override def doAction(
          context: FlightProducer.CallContext,
          action: Action,
          listener: FlightProducer.StreamListener[Result]
      ): Unit = {
        val respJson = action.getType match {
          case "init_job" | "reset_job" | "complete_job" | "fail_job" =>
            s"""{"action":"${action.getType}","status":"ok"}"""
          case "get_job_status" =>
            val jid = new String(action.getBody, StandardCharsets.UTF_8)
            s"""{"job_id":"$jid","state":"completed","total_rows_written":${store.size()}}"""
          case "get_cluster_status" =>
            """{"node_id":1,"role":"Leader","term":1}"""
          case _ =>
            "{}"
        }
        listener.onNext(new Result(respJson.getBytes(StandardCharsets.UTF_8)))
        listener.onCompleted()
      }
    }

    val headerAuth = new org.apache.arrow.flight.auth2.GeneratedBearerTokenAuthenticator(
      new org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator(
        new org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator.CredentialValidator {
          override def validate(username: String, password: String): org.apache.arrow.flight.auth2.CallHeaderAuthenticator.AuthResult = {
            new org.apache.arrow.flight.auth2.CallHeaderAuthenticator.AuthResult {
              override def getPeerIdentity: String = username
            }
          }
        }
      )
    )

    val server = FlightServer
      .builder(allocator, Location.forGrpcInsecure("127.0.0.1", 0), producer)
      .headerAuthenticator(headerAuth)
      .build()
      .start()

    val hostPort = s"127.0.0.1:${server.getPort}"
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("ArrowBatchConverterSpec")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    try {
      import spark.implicits._
      AerospikeLoadStatus.clear()

      val baseOpts = Map(
        "connector.jobId" -> "base-test-job",
        "flight.hosts" -> hostPort,
        "flight.username" -> "spark_user",
        "flight.password" -> "spark_pass",
        "aerospike.namespace" -> "test",
        "aerospike.set" -> "users",
        "aerospike.keyColumn" -> "pk",
        "connector.batchSize" -> "2",
        "connector.writerThreads" -> "2",
        "aerospike.read.numPartitions" -> "2",
        "aerospike.read.batchSize" -> "2"
      )

      // 1. Write 4 rows via DataSource V2 (`df.write.format("aerospike-flight")`)
      val writeDf = Seq(
        ("u_1", 25, 10.5),
        ("u_2", 30, 20.0),
        ("u_3", 35, 30.25),
        ("u_4", 40, 40.75)
      ).toDF("pk", "age", "score")

      writeDf.write
        .format("com.asproxy.spark.AerospikeFlightDataSource")
        .options(baseOpts + ("connector.jobId" -> "job-write-1"))
        .mode("overwrite")
        .save()

      assert(store.size() == 4)
      val writeSummary = AerospikeLoadStatus.getSummary("job-write-1").get
      assert(writeSummary.rowsWritten == 4L)
      assert(writeSummary.status == "COMPLETED")
      assert(AerospikeLoadStatus.allSummaries.nonEmpty)

      // 2. DQ validation write (`aerospike.dq -> "true"`)
      writeDf.write
        .format("com.asproxy.spark.AerospikeFlightDataSource")
        .options(baseOpts + ("connector.jobId" -> "job-dq-1", "aerospike.dq" -> "true"))
        .mode("append")
        .save()

      val dqSummary = AerospikeLoadStatus.getSummary("job-dq-1").get
      assert(dqSummary.validateOnly)
      assert(dqSummary.rowsValidated == 4L)
      assert(dqSummary.dqPassed.contains(true))

      // 3. Query Remote Job Status and Cluster Status via DoAction
      val remoteJobJson = AerospikeLoadStatus.fetchRemoteJobStatusJson(hostPort, "job-write-1", "spark_user", "spark_pass")
      assert(remoteJobJson.contains("\"job_id\":\"job-write-1\""))
      val clusterJson = AerospikeLoadStatus.fetchClusterStatusJson(hostPort, "spark_user", "spark_pass")
      assert(clusterJson.contains("\"role\":\"Leader\""))

      // 4. Read with Schema Inference (including fallback from unreachable first endpoint `127.0.0.1:1`)
      val fallbackHosts = s"127.0.0.1:1,$hostPort"
      val inferredReadDf = spark.read
        .format("com.asproxy.spark.AerospikeFlightDataSource")
        .options(baseOpts.updated("flight.hosts", fallbackHosts))
        .load()

      assert(inferredReadDf.schema.fieldNames.toSeq == Seq("pk", "age", "score"))
      assert(inferredReadDf.count() == 4L) // 0-column pruned count() query!
      val prunedRows = inferredReadDf.select("pk", "age").collect().map(r => r.getString(0) -> r.getInt(1)).toMap
      assert(prunedRows == Map("u_1" -> 25, "u_2" -> 30, "u_3" -> 35, "u_4" -> 40))

      // 5. Read with PK Filter Pushdown (`=`, `<=>`, `IN`, `AND`) & `aerospike.read.keys`
      val explicitSchema = new StructType()
        .add("pk", StringType, nullable = false)
        .add("age", IntegerType, nullable = true)
        .add("score", DoubleType, nullable = true)

      val eqRows = spark.read
        .format("com.asproxy.spark.AerospikeFlightDataSource")
        .schema(explicitSchema)
        .options(baseOpts)
        .load()
        .filter("pk = 'u_2'")
        .collect()
      assert(eqRows.length == 1 && eqRows(0).getInt(1) == 30)

      val nullSafeRows = spark.read
        .format("com.asproxy.spark.AerospikeFlightDataSource")
        .schema(explicitSchema)
        .options(baseOpts)
        .load()
        .filter("pk <=> 'u_3' AND (pk = 'u_3' OR pk = 'u_4')")
        .collect()
      assert(nullSafeRows.length == 1 && nullSafeRows(0).getInt(1) == 35)

      val emptyIntersectionRows = spark.read
        .format("com.asproxy.spark.AerospikeFlightDataSource")
        .schema(explicitSchema)
        .options(baseOpts)
        .load()
        .filter("pk = 'u_1' AND pk = 'u_2'")
        .collect()
      assert(emptyIntersectionRows.isEmpty)

      val optionKeysRows = spark.read
        .format("com.asproxy.spark.AerospikeFlightDataSource")
        .schema(explicitSchema)
        .options(baseOpts + ("aerospike.read.keys" -> "u_1,u_4"))
        .load()
        .filter("pk IN ('u_1', 'u_2')")
        .collect()
      assert(optionKeysRows.length == 1 && optionKeysRows(0).getString(0) == "u_1")

      // 6. Distributed DoExchange Lookup (`AerospikeFlightLookup.lookup` & `aerospike.lookup.sourceView`)
      val keysDf = Seq(("req_1", "u_1"), ("req_2", "u_missing"), ("req_3", "u_4")).toDF("req_id", "pk")

      val leftEnriched = AerospikeFlightLookup
        .lookup(keysDf, "age INT, score DOUBLE", (baseOpts + ("aerospike.lookup.joinType" -> "left")).asJava)
        .collect()
        .map(r => r.getString(0) -> (if (r.isNullAt(2)) None else Some(r.getInt(2))))
        .toMap
      assert(leftEnriched == Map("req_1" -> Some(25), "req_2" -> None, "req_3" -> Some(40)))

      val innerEnriched = AerospikeFlightLookup
        .lookup(
          keysDf,
          new StructType().add("age", IntegerType).add("score", DoubleType),
          (baseOpts + ("aerospike.lookup.joinType" -> "inner")).asJava
        )
        .collect()
      assert(innerEnriched.length == 2)

      // Lookup via `spark.read.format(...).option("aerospike.lookup.sourceView", ...)`
      keysDf.createOrReplaceTempView("lookup_keys_view")
      val viewLookupDf = spark.read
        .format("com.asproxy.spark.AerospikeFlightDataSource")
        .options(
          baseOpts + (
            "aerospike.lookup.sourceView" -> "lookup_keys_view",
            "aerospike.lookup.binsSchema" -> "age INT, score DOUBLE"
          )
        )
        .load()
      assert(viewLookupDf.collect().length == 3)

      val viewLookupWithSchemaDf = spark.read
        .format("com.asproxy.spark.AerospikeFlightDataSource")
        .schema(new StructType().add("age", IntegerType).add("score", DoubleType))
        .options(baseOpts + ("aerospike.lookup.sourceView" -> "lookup_keys_view"))
        .load()
      assert(viewLookupWithSchemaDf.collect().length == 3)

      // 7. Exercise BatchWrite.abort, StreamingWrite, and DataWriter metrics/abort directly
      val parsedOpts = AerospikeFlightOptions.fromMap(baseOpts + ("connector.jobId" -> "job-stream-1"))
      val wb = new AerospikeFlightWriteBuilder(explicitSchema, parsedOpts).truncate().build()
      val bw = wb.toBatch.asInstanceOf[AerospikeFlightBatchWrite]
      bw.abort(Array.empty)
      assert(AerospikeLoadStatus.getSummary("job-stream-1").get.status == "FAILED")

      val sw = wb.toStreaming.asInstanceOf[AerospikeFlightStreamingWrite]
      val swFactory = sw.createStreamingWriterFactory(new PhysicalWriteInfo {
        override def numPartitions(): Int = 1
      })
      val writer = swFactory.createWriter(0, 1L, 0L)
      assert(writer.currentMetricsValues().length == 7)
      writer.abort()
      writer.close()
      sw.commit(0L, Array.empty)
      sw.abort(0L, Array.empty)

      // 8. Verify all Flight compression modes ("zstd", "lz4", "gzip", "none") and disableable switch
      for (compMode <- Seq("zstd", "lz4", "gzip", "none")) {
        val compOpts = baseOpts + (
          "connector.jobId" -> s"job-comp-$compMode",
          "flight.compression" -> compMode
        )
        writeDf.write
          .format("com.asproxy.spark.AerospikeFlightDataSource")
          .options(compOpts)
          .mode("append")
          .save()

        val readCompRows = spark.read
          .format("com.asproxy.spark.AerospikeFlightDataSource")
          .schema(explicitSchema)
          .options(compOpts)
          .load()
          .collect()
        assert(readCompRows.length == 4)

        val lookupCompRows = AerospikeFlightLookup
          .lookup(keysDf, "age INT, score DOUBLE", compOpts)
          .collect()
        assert(lookupCompRows.length == 3)
      }

      // Also verify `flight.compression.enabled = "false"` disables compression even if `flight.compression = "zstd"`
      val disabledCompOpts = baseOpts + (
        "connector.jobId" -> "job-comp-disabled",
        "flight.compression" -> "zstd",
        "flight.compression.enabled" -> "false"
      )
      assert(AerospikeFlightOptions.fromMap(disabledCompOpts).compression == "none")
      writeDf.write
        .format("com.asproxy.spark.AerospikeFlightDataSource")
        .options(disabledCompOpts)
        .mode("append")
        .save()
    } finally {
      spark.stop()
      server.close()
      allocator.close()
    }
  }

  test("ArrowCompressionCodecs & FlightCompressionHelper: ZSTD, LZ4_FRAME, Gzip, and disableable compression") {
    import org.apache.arrow.vector.compression.{CompressionUtil, NoCompressionCodec}

    val allocator = new RootAllocator(Long.MaxValue)
    try {
      // 1. FlightCompressionHelper.normalizeCompression
      assert(FlightCompressionHelper.normalizeCompression("zstd", enabled = true) == "zstd")
      assert(FlightCompressionHelper.normalizeCompression("ZSTANDARD", enabled = true) == "zstd")
      assert(FlightCompressionHelper.normalizeCompression("true", enabled = true) == "zstd")
      assert(FlightCompressionHelper.normalizeCompression("lz4", enabled = true) == "lz4")
      assert(FlightCompressionHelper.normalizeCompression("LZ4_FRAME", enabled = true) == "lz4")
      assert(FlightCompressionHelper.normalizeCompression("gzip", enabled = true) == "gzip")
      assert(FlightCompressionHelper.normalizeCompression("none", enabled = true) == "none")
      assert(FlightCompressionHelper.normalizeCompression("off", enabled = true) == "none")
      assert(FlightCompressionHelper.normalizeCompression("false", enabled = true) == "none")
      assert(FlightCompressionHelper.normalizeCompression("disabled", enabled = true) == "none")
      assert(FlightCompressionHelper.normalizeCompression("zstd", enabled = false) == "none")
      intercept[IllegalArgumentException] {
        FlightCompressionHelper.normalizeCompression("brotli", enabled = true)
      }

      // 2. grpcCompressionCallOption
      assert(FlightCompressionHelper.grpcCompressionCallOption("gzip").isDefined)
      assert(FlightCompressionHelper.grpcCompressionCallOption("zstd").isEmpty)
      assert(FlightCompressionHelper.grpcCompressionCallOption("none").isEmpty)

      // 3. ArrowCompressionCodecFactory & direct ZSTD / LZ4_FRAME buffer round-trip
      val zstdCodec = ArrowCompressionCodecFactory.createCodec(CompressionUtil.CodecType.ZSTD)
      val lz4Codec = ArrowCompressionCodecFactory.createCodec(CompressionUtil.CodecType.LZ4_FRAME)
      assert(zstdCodec.getCodecType == CompressionUtil.CodecType.ZSTD)
      assert(lz4Codec.getCodecType == CompressionUtil.CodecType.LZ4_FRAME)
      assert(NoCompressionCodec.Factory.INSTANCE != null)

      val payloadBytes = ("aerospike-arrow-flight-high-speed-compression-test-" * 64).getBytes("UTF-8")
      for (codec <- Seq(zstdCodec, lz4Codec)) {
        val rawBuf = allocator.buffer(payloadBytes.length)
        rawBuf.writeBytes(payloadBytes)
        // compress() closes rawBuf and returns compressedBuf
        val compressedBuf = codec.compress(allocator, rawBuf)
        assert(compressedBuf.readableBytes() < payloadBytes.length)

        // decompress() closes compressedBuf and returns decompressedBuf
        val decompressedBuf = codec.decompress(allocator, compressedBuf)
        assert(decompressedBuf.readableBytes() == payloadBytes.length)
        val outBytes = new Array[Byte](payloadBytes.length)
        decompressedBuf.getBytes(0, outBytes)
        assert(java.util.Arrays.equals(payloadBytes, outBytes))
        decompressedBuf.close()

        // Also test tiny incompressible buffer (AbstractCompressionCodec uncompressed fallback path)
        val tinyBytes = Array[Byte](1, 2, 3)
        val tinyBuf = allocator.buffer(tinyBytes.length)
        tinyBuf.writeBytes(tinyBytes)
        val tinyComp = codec.compress(allocator, tinyBuf)
        val tinyDecomp = codec.decompress(allocator, tinyComp)
        val tinyOut = new Array[Byte](tinyBytes.length)
        tinyDecomp.getBytes(0, tinyOut)
        assert(java.util.Arrays.equals(tinyBytes, tinyOut))
        tinyDecomp.close()
      }
    } finally {
      allocator.close()
    }
  }
}
