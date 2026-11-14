package com.asproxy.spark

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.arrow.flight.{FlightClient, FlightDescriptor}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkInternalBridge, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.types._

import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import scala.collection.JavaConverters._

/**
 * Distributed MPP key-lookup enrichment over Apache Arrow Flight `DoExchange`.
 *
 * Streams partitions of an existing `DataFrame` containing primary keys directly from Spark
 * Executors to `as_proxy_server` nodes over bidirectional Arrow Flight `DoExchange` without
 * collecting keys to the Spark Driver or shuffling data.
 */
object AerospikeFlightLookup extends Logging {

  trait CloseableRowIterator extends Iterator[InternalRow] with AutoCloseable

  private val EmptyCloseableRowIterator: CloseableRowIterator = new CloseableRowIterator {
    override def hasNext: Boolean = false
    override def next(): InternalRow = throw new NoSuchElementException("Empty lookup partition")
    override def close(): Unit = ()
  }

  /**
   * Enrich `keysDf` by fetching the specified `binsSchema` columns from Aerospike for each row's
   * primary key (`aerospike.keyColumn`).
   *
   * Supports both `"left"` join (all rows from `keysDf` are preserved; missing keys get `null` bins)
   * and `"inner"` join (only rows whose key exists in Aerospike are returned) via `aerospike.lookup.joinType`.
   */
  def lookup(
      keysDf: DataFrame,
      binsSchema: StructType,
      options: Map[String, String]
  ): DataFrame = {
    val parsedOpts = AerospikeFlightOptions.fromTableOrReadMap(options)
    lookupWithOptions(keysDf, binsSchema, parsedOpts)
  }

  /**
   * Overload accepting `binsSchemaDdlOrJson` as a Spark DDL string (e.g. `"age INT, score DOUBLE"`)
   * or JSON schema string, and a Scala `Map` of options.
   */
  def lookup(
      keysDf: DataFrame,
      binsSchemaDdlOrJson: String,
      options: Map[String, String]
  ): DataFrame = {
    val parsedSchema = parseBinsSchema(binsSchemaDdlOrJson)
    lookup(keysDf, parsedSchema, options)
  }

  /**
   * PySpark / Java-friendly overload accepting `binsSchemaDdlOrJson` (DDL or JSON string)
   * and a `java.util.Map[String, String]` of options.
   *
   * Example from PySpark:
   * {{{
   *   from pyspark.sql import DataFrame
   *   jdf = spark._jvm.com.asproxy.spark.AerospikeFlightLookup.lookup(
   *       keys_df._jdf,
   *       "age INT, score DOUBLE, tags ARRAY<STRING>",
   *       {"flight.hosts": "proxy-1:8815", "aerospike.namespace": "test", "aerospike.set": "users", "aerospike.keyColumn": "pk"}
   *   )
   *   enriched_df = DataFrame(jdf, spark)
   * }}}
   */
  def lookup(
      keysDf: DataFrame,
      binsSchemaDdlOrJson: String,
      options: util.Map[String, String]
  ): DataFrame = {
    val parsedSchema = parseBinsSchema(binsSchemaDdlOrJson)
    lookup(keysDf, parsedSchema, options.asScala.toMap)
  }

  /**
   * Java-friendly overload accepting `StructType` and `java.util.Map[String, String]`.
   */
  def lookup(
      keysDf: DataFrame,
      binsSchema: StructType,
      options: util.Map[String, String]
  ): DataFrame = {
    lookup(keysDf, binsSchema, options.asScala.toMap)
  }

  private def withJoinTypeOption(
      options: scala.collection.Map[String, String],
      joinType: String
  ): Map[String, String] = {
    val cleaned = options.filterNot { case (k, _) =>
      val lower = k.toLowerCase
      lower == "aerospike.lookup.jointype" || lower == "aerospike.lookup.join_type"
    }.toMap
    cleaned + ("aerospike.lookup.joinType" -> joinType)
  }

  /**
   * Convenience `enrich` overloads matching PySpark (`PythonUtils.toScalaMap` or `java.util.Map`)
   * and Scala callers with `(keysDf, options, binsSchema[, joinType])` argument order.
   */
  def enrich(
      keysDf: DataFrame,
      options: scala.collection.Map[String, String],
      binsSchema: StructType,
      joinType: String
  ): DataFrame = {
    lookup(keysDf, binsSchema, withJoinTypeOption(options, joinType))
  }

  def enrich(
      keysDf: DataFrame,
      options: scala.collection.Map[String, String],
      binsSchema: StructType
  ): DataFrame = {
    lookup(keysDf, binsSchema, options.toMap)
  }

  def enrich(
      keysDf: DataFrame,
      options: scala.collection.Map[String, String],
      binsSchemaDdlOrJson: String,
      joinType: String
  ): DataFrame = {
    lookup(keysDf, parseBinsSchema(binsSchemaDdlOrJson), withJoinTypeOption(options, joinType))
  }

  def enrich(
      keysDf: DataFrame,
      options: scala.collection.Map[String, String],
      binsSchemaDdlOrJson: String
  ): DataFrame = {
    lookup(keysDf, parseBinsSchema(binsSchemaDdlOrJson), options.toMap)
  }

  def enrich(
      keysDf: DataFrame,
      options: util.Map[String, String],
      binsSchema: StructType,
      joinType: String
  ): DataFrame = {
    lookup(keysDf, binsSchema, withJoinTypeOption(options.asScala, joinType))
  }

  def enrich(
      keysDf: DataFrame,
      options: util.Map[String, String],
      binsSchema: StructType
  ): DataFrame = {
    lookup(keysDf, binsSchema, options.asScala.toMap)
  }

  def enrich(
      keysDf: DataFrame,
      options: util.Map[String, String],
      binsSchemaDdlOrJson: String,
      joinType: String
  ): DataFrame = {
    lookup(keysDf, parseBinsSchema(binsSchemaDdlOrJson), withJoinTypeOption(options.asScala, joinType))
  }

  def enrich(
      keysDf: DataFrame,
      options: util.Map[String, String],
      binsSchemaDdlOrJson: String
  ): DataFrame = {
    lookup(keysDf, parseBinsSchema(binsSchemaDdlOrJson), options.asScala.toMap)
  }

  def parseBinsSchema(ddlOrJson: String): StructType = {
    val trimmed = Option(ddlOrJson).map(_.trim).getOrElse("")
    require(trimmed.nonEmpty, "binsSchema string cannot be empty")
    if (trimmed.startsWith("{")) {
      DataType.fromJson(trimmed).asInstanceOf[StructType]
    } else {
      StructType.fromDDL(trimmed)
    }
  }

  def lookupWithOptions(
      keysDf: DataFrame,
      rawBinsSchema: StructType,
      options: AerospikeFlightOptions
  ): DataFrame = {
    options.validateReadOrLookupTarget()
    val inputSchema = keysDf.schema
    if (!inputSchema.fields.exists(_.name == options.keyColumn)) {
      throw new IllegalArgumentException(
        s"Primary key column '${options.keyColumn}' not found in input DataFrame schema: [${inputSchema.fields.map(_.name).mkString(", ")}]"
      )
    }

    val keyField = inputSchema.fields.find(_.name == options.keyColumn).get

    // Remove keyColumn from binsSchema if the caller included it, and validate with AerospikeSchemaValidator
    val effectiveBinsFields = rawBinsSchema.fields.filterNot(_.name == options.keyColumn)
    if (effectiveBinsFields.isEmpty) {
      throw new IllegalArgumentException(
        "binsSchema must contain at least 1 bin column to fetch from Aerospike"
      )
    }
    val binsOnlySchema = StructType(effectiveBinsFields)

    // Validate PK + requested bins against Aerospike rules (bin name length <= 15, valid types, etc.)
    val validationSchema = StructType(keyField +: effectiveBinsFields)
    AerospikeSchemaValidator.validate(validationSchema, options, requireWriteJobId = false)

    val outputSchema = buildOutputSchema(inputSchema, binsOnlySchema, options.keyColumn, options.lookupJoinType)

    logInfo(
      s"[Driver][Lookup] Planning distributed DoExchange lookup on '${options.namespace}.${options.setName}': " +
        s"keyColumn='${options.keyColumn}', joinType='${options.lookupJoinType}', " +
        s"inputColumns=[${inputSchema.fields.map(_.name).mkString(", ")}], " +
        s"lookupBins=[${binsOnlySchema.fields.map(_.name).mkString(", ")}], " +
        s"outputSchema=[${outputSchema.toDDL}]"
    )

    val spark = keysDf.sparkSession
    val inputRdd = keysDf.queryExecution.toRdd

    val enrichedInternalRdd = inputRdd.mapPartitionsWithIndex { (partitionIdx, rowIter) =>
      val internalIter = enrichPartitionIteratorInternal(
        partitionIndex = partitionIdx,
        inputRows = rowIter,
        inputSchema = inputSchema,
        binsOnlySchema = binsOnlySchema,
        outputSchema = outputSchema,
        options = options,
        copyRows = false
      )
      val unsafeProj = UnsafeProjection.create(outputSchema)
      internalIter.map(row => unsafeProj(row): InternalRow)
    }

    SparkInternalBridge.createDataFrame(spark, enrichedInternalRdd, outputSchema)
  }

  def buildOutputSchema(
      inputSchema: StructType,
      binsOnlySchema: StructType,
      keyColumn: String,
      joinType: String
  ): StructType = {
    val forceNullable = joinType == "left"
    val appendedBinFields = binsOnlySchema.fields
      .filterNot(f => f.name == keyColumn || inputSchema.fields.exists(_.name == f.name))
      .map(f => if (forceNullable) f.copy(nullable = true) else f)
    StructType(inputSchema.fields ++ appendedBinFields)
  }

  /**
   * Streams `inputRows` in Arrow batches over `DoExchange` to `as_proxy_server` and returns
   * a closeable iterator of enriched `InternalRow`s matching `outputSchema`.
   */
  def enrichPartitionIterator(
      partitionIndex: Int,
      inputRows: Iterator[InternalRow],
      inputSchema: StructType,
      binsOnlySchema: StructType,
      outputSchema: StructType,
      options: AerospikeFlightOptions
  ): CloseableRowIterator = {
    enrichPartitionIteratorInternal(
      partitionIndex = partitionIndex,
      inputRows = inputRows,
      inputSchema = inputSchema,
      binsOnlySchema = binsOnlySchema,
      outputSchema = outputSchema,
      options = options,
      copyRows = true
    )
  }

  private[spark] def enrichPartitionIteratorInternal(
      partitionIndex: Int,
      inputRows: Iterator[InternalRow],
      inputSchema: StructType,
      binsOnlySchema: StructType,
      outputSchema: StructType,
      options: AerospikeFlightOptions,
      copyRows: Boolean
  ): CloseableRowIterator = {
    if (!inputRows.hasNext) {
      logDebug(s"[Executor][Lookup] Skipping empty partition=$partitionIndex for '${options.namespace}.${options.setName}'")
      return EmptyCloseableRowIterator
    }

    val startTimeMs = System.currentTimeMillis()
    val allocator: BufferAllocator = new RootAllocator(Long.MaxValue)
    val descriptorBytes = buildExchangeDescriptorJsonBytes(binsOnlySchema, options)
    val descriptor = FlightDescriptor.command(descriptorBytes)

    var flightClient: FlightClient = null
    var exchange: FlightClient.ExchangeReaderWriter = null
    var connectedEndpointStr: String = "unconnected"

    var lastErr: Throwable = null
    var connected = false
    var attempt = 0
    while (attempt < options.hosts.size && !connected) {
      val endpoint = options.endpointFor(partitionIndex, attempt)
      try {
        flightClient = AerospikeLoadStatus.createFlightClient(allocator, endpoint, options)
        val callOptions = AerospikeLoadStatus.buildCallOptions(flightClient, options)
        exchange = flightClient.doExchange(descriptor, callOptions: _*)
        connectedEndpointStr = s"${endpoint.host}:${endpoint.port}"
        connected = true
        logInfo(
          s"[Executor][Lookup] Opened DoExchange stream for partition=$partitionIndex on endpoint=$connectedEndpointStr, " +
            s"target='${options.namespace}.${options.setName}', joinType='${options.lookupJoinType}', " +
            s"compression='${options.compression}', lookupBins=[${binsOnlySchema.fields.map(_.name).mkString(", ")}]"
        )
      } catch {
        case t: Throwable =>
          lastErr = t
          logWarning(
            s"[Executor][Lookup] Failed to open DoExchange stream for partition=$partitionIndex " +
              s"on ${endpoint.host}:${endpoint.port} (attempt ${attempt + 1}/${options.hosts.size}): ${t.getMessage}"
          )
          if (exchange != null) {
            try { exchange.close() } catch { case _: Throwable => }
            exchange = null
          }
          if (flightClient != null) {
            try { flightClient.close() } catch { case _: Throwable => }
            flightClient = null
          }
          attempt += 1
      }
    }

    if (!connected) {
      try { allocator.close() } catch { case _: Throwable => }
      val errMsg =
        s"Failed to open Arrow Flight DoExchange stream for partition $partitionIndex across endpoints ${options.hosts.map(h => s"${h.host}:${h.port}").mkString(", ")}"
      logError(s"[Executor][Lookup] $errMsg", lastErr)
      throw new RuntimeException(errMsg, lastErr)
    }

    val senderError = new AtomicReference[Throwable](null)
    val inputRowsSent = new AtomicLong(0L)
    val inputBatchesSent = new AtomicLong(0L)
    val outputRowsReceived = new AtomicLong(0L)
    val outputBatchesReceived = new AtomicLong(0L)
    @volatile var cancelled = false

    val senderAllocator = allocator.newChildAllocator(s"lookup-sender-p$partitionIndex", 0L, Long.MaxValue)
    val inputArrowSchema = ArrowSchemaConverter.toArrowSchema(inputSchema)
    val inputRoot = VectorSchemaRoot.create(inputArrowSchema, senderAllocator)
    val batchConverter = new ArrowBatchConverter(
      sparkSchema = inputSchema,
      keyColumn = options.keyColumn,
      root = inputRoot,
      ignoreNulls = false,
      validateOnly = true
    )

    val writer = exchange.getWriter
    val reader = exchange.getReader
    FlightCompressionHelper.enableStreamDecompression(reader)

    val senderThread = new Thread(
      new Runnable {
        override def run(): Unit = {
          try {
            writer.start(inputRoot)
            FlightCompressionHelper.configureOutboundStream(
              writer,
              inputRoot,
              options.compression,
              options.compressionLevel
            )
            batchConverter.reset()
            while (!cancelled && inputRows.hasNext) {
              val row = inputRows.next()
              batchConverter.appendRow(row)
              inputRowsSent.incrementAndGet()
              if (batchConverter.rowCount >= options.readBatchSize) {
                batchConverter.finishBatch()
                if (!cancelled) {
                  writer.putNext()
                  inputBatchesSent.incrementAndGet()
                }
                batchConverter.reset()
              }
            }
            if (!cancelled && batchConverter.rowCount > 0) {
              batchConverter.finishBatch()
              writer.putNext()
              inputBatchesSent.incrementAndGet()
            }
            if (!cancelled) {
              writer.completed()
            }
          } catch {
            case t: Throwable =>
              if (!cancelled) {
                logError(
                  s"[Executor][Lookup] DoExchange sender thread failed in partition=$partitionIndex on $connectedEndpointStr: ${t.getMessage}",
                  t
                )
                senderError.compareAndSet(null, t)
                try { writer.error(t) } catch { case _: Throwable => }
              }
          } finally {
            try { inputRoot.close() } catch { case _: Throwable => }
            try { senderAllocator.close() } catch { case _: Throwable => }
          }
        }
      },
      s"as-flight-lookup-sender-p$partitionIndex"
    )
    senderThread.setDaemon(true)
    senderThread.start()

    @volatile var closed = false
    val closeLock = new AnyRef
    def closeAll(): Unit = {
      closeLock.synchronized {
        if (!closed) {
          closed = true
          cancelled = true
          // Cancel reader and close exchange first to unblock senderThread if waiting on gRPC flow control
          if (reader != null) {
            try { reader.cancel("Lookup stream closed", null) } catch { case _: Throwable => }
          }
          if (exchange != null) {
            try { exchange.close() } catch { case _: Throwable => }
          }
          if (Thread.currentThread() ne senderThread) {
            try { senderThread.interrupt() } catch { case _: Throwable => }
            try { senderThread.join(5000L) } catch { case _: Throwable => }
          }
          if (flightClient != null) {
            try { flightClient.close() } catch { case _: Throwable => }
          }
          try { allocator.close() } catch { case _: Throwable => }
          val elapsedMs = math.max(0L, System.currentTimeMillis() - startTimeMs)
          logInfo(
            s"[Executor][Lookup] Closed DoExchange stream for partition=$partitionIndex on $connectedEndpointStr: " +
              s"inputRowsSent=${inputRowsSent.get()}, inputBatchesSent=${inputBatchesSent.get()}, " +
              s"outputRowsReceived=${outputRowsReceived.get()}, outputBatchesReceived=${outputBatchesReceived.get()}, " +
              s"elapsedMs=$elapsedMs"
          )
        }
      }
    }

    Option(TaskContext.get()).foreach { tc =>
      tc.addTaskCompletionListener[Unit](_ => closeAll())
    }

    new CloseableRowIterator {
      private var rowReader: ArrowRowReader = _
      private var currentIdx: Int = 0
      private var currentCount: Int = 0
      private var finished: Boolean = false

      override def hasNext: Boolean = {
        if (finished || closed) return false
        while (currentIdx >= currentCount) {
          val sendErr = senderError.get()
          if (sendErr != null) {
            closeAll()
            throw new RuntimeException("Arrow Flight DoExchange sender failed", sendErr)
          }
          val hasMoreBatches =
            try {
              reader.next()
            } catch {
              case t: Throwable =>
                val cause = Option(senderError.get()).getOrElse(t)
                closeAll()
                throw new RuntimeException(s"Arrow Flight DoExchange reader failed: ${cause.getMessage}", cause)
            }
          if (!hasMoreBatches) {
            val postSendErr = senderError.get()
            closeAll()
            if (postSendErr != null) {
              throw new RuntimeException("Arrow Flight DoExchange sender failed", postSendErr)
            }
            finished = true
            return false
          }
          val root = reader.getRoot
          if (rowReader == null) {
            rowReader = new ArrowRowReader(outputSchema, root)
          }
          currentIdx = 0
          currentCount = root.getRowCount
          if (currentCount > 0) {
            outputBatchesReceived.incrementAndGet()
          }
        }
        true
      }

      override def next(): InternalRow = {
        if (!hasNext) {
          throw new NoSuchElementException("End of DoExchange lookup stream")
        }
        val rawRow = rowReader.readRow(currentIdx)
        val row = if (copyRows) rawRow.copy() else rawRow
        currentIdx += 1
        outputRowsReceived.incrementAndGet()
        row
      }

      override def close(): Unit = {
        finished = true
        closeAll()
      }
    }
  }

  private def buildExchangeDescriptorJsonBytes(
      binsOnlySchema: StructType,
      options: AerospikeFlightOptions
  ): Array[Byte] = {
    val map = new util.LinkedHashMap[String, Any]()
    map.put("namespace", options.namespace)
    map.put("set_name", options.setName)
    map.put("key_column", options.keyColumn)
    map.put("join_type", options.lookupJoinType)
    map.put("type_mismatch_mode", options.readTypeMismatchMode)
    map.put("bins_schema_ipc_base64", ArrowSchemaConverter.toArrowSchemaIpcBase64(binsOnlySchema))
    map.put("compression", options.compression)
    map.put("compression_level", Int.box(options.compressionLevel))
    AerospikeFlightOptions.SharedMapper.writeValueAsBytes(map)
  }
}
