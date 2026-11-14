package com.asproxy.spark

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.arrow.flight.{FlightClient, FlightDescriptor, FlightStream, Ticket}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import java.nio.charset.StandardCharsets
import java.util
import java.util.Base64
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Serializable representation of a primary key literal for `BatchKeys` reads.
 */
sealed trait KeyLiteralSpec extends Serializable {
  def toMap: util.Map[String, Any]
}

object KeyLiteralSpec {
  case class StringKey(value: String) extends KeyLiteralSpec {
    override def toMap: util.Map[String, Any] = {
      val m = new util.LinkedHashMap[String, Any]()
      m.put("type", "string")
      m.put("value", value)
      m
    }
  }

  case class IntKey(value: Long) extends KeyLiteralSpec {
    override def toMap: util.Map[String, Any] = {
      val m = new util.LinkedHashMap[String, Any]()
      m.put("type", "int")
      m.put("value", Long.box(value))
      m
    }
  }

  case class BytesKey(value: Array[Byte]) extends KeyLiteralSpec {
    override def toMap: util.Map[String, Any] = {
      val m = new util.LinkedHashMap[String, Any]()
      m.put("type", "bytes")
      val list = new util.ArrayList[Int](value.length)
      var i = 0
      while (i < value.length) {
        list.add(value(i) & 0xff)
        i += 1
      }
      m.put("value", list)
      m
    }
  }

  def fromSparkValue(v: Any, keyDataType: DataType): Option[KeyLiteralSpec] = {
    if (v == null) return None
    keyDataType match {
      case ByteType | ShortType | IntegerType | LongType =>
        v match {
          case n: Number => Some(IntKey(n.longValue()))
          case s: String => scala.util.Try(s.trim.toLong).toOption.map(IntKey)
          case _         => None
        }
      case BinaryType =>
        v match {
          case b: Array[Byte] => Some(BytesKey(b))
          case s: String      => Some(BytesKey(Base64.getDecoder.decode(s.trim)))
          case _              => None
        }
      case _ =>
        Some(StringKey(v.toString))
    }
  }

  def parseOptionKeys(rawKeys: String, keyDataType: DataType): Seq[KeyLiteralSpec] = {
    val trimmed = rawKeys.trim
    if (trimmed.isEmpty) return Seq.empty
    if (trimmed.startsWith("[")) {
      val node = AerospikeFlightOptions.SharedMapper.readTree(trimmed)
      if (node != null && node.isArray) {
        val buf = ArrayBuffer.empty[KeyLiteralSpec]
        val it = node.elements()
        while (it.hasNext) {
          val elem = it.next()
          if (elem.isIntegralNumber) {
            buf += IntKey(elem.asLong())
          } else if (elem.isTextual) {
            fromSparkValue(elem.asText(), keyDataType).foreach(buf += _)
          }
        }
        return buf.toSeq
      }
    }
    trimmed
      .split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
      .flatMap(s => fromSparkValue(s, keyDataType))
      .toSeq
  }
}

/**
 * Driver-side helper that infers a Spark `StructType` by sampling up to `sampleSize` records
 * from Aerospike via the Rust proxy's Arrow Flight `GetSchema` RPC.
 */
object AerospikeFlightSchemaInferrer extends Logging {
  def inferSchema(options: AerospikeFlightOptions): StructType = {
    options.validateReadOrLookupTarget()
    val startMs = System.currentTimeMillis()
    val reqMap = new util.LinkedHashMap[String, Any]()
    reqMap.put("namespace", options.namespace)
    reqMap.put("set_name", options.setName)
    reqMap.put("key_column", options.keyColumn)
    reqMap.put("sample_size", Int.box(options.readSampleSize))
    val cmdBytes = AerospikeFlightOptions.SharedMapper.writeValueAsBytes(reqMap)

    var lastErr: Throwable = null
    var idx = 0
    while (idx < options.hosts.size) {
      val endpoint = options.hosts(idx)
      val allocator: BufferAllocator = new RootAllocator(Long.MaxValue)
      var client: FlightClient = null
      try {
        client = AerospikeLoadStatus.createFlightClient(allocator, endpoint, options)
        val callOptions = AerospikeLoadStatus.buildCallOptions(client, options)
        val schemaResult = client.getSchema(FlightDescriptor.command(cmdBytes), callOptions: _*)
        val inferred = ArrowSchemaConverter.fromArrowSchema(schemaResult.getSchema)
        val elapsedMs = math.max(0L, System.currentTimeMillis() - startMs)
        logInfo(
          s"[Driver][InferSchema] Inferred ${inferred.fields.length} column(s) from '${options.namespace}.${options.setName}' " +
            s"via ${endpoint.host}:${endpoint.port} (sampleSize=${options.readSampleSize}) in ${elapsedMs}ms: [${inferred.toDDL}]"
        )
        return inferred
      } catch {
        case scala.util.control.NonFatal(t) =>
          lastErr = t
          logWarning(
            s"[Driver][InferSchema] Failed to infer schema from ${endpoint.host}:${endpoint.port} " +
              s"(attempt ${idx + 1}/${options.hosts.size}): ${t.getMessage}"
          )
      } finally {
        if (client != null) {
          try { client.close() } catch { case _: Throwable => }
        }
        try { allocator.close() } catch { case _: Throwable => }
      }
      idx += 1
    }
    val errMsg =
      s"Failed to infer schema from Aerospike via Arrow Flight endpoints ${options.hosts.map(h => s"${h.host}:${h.port}").mkString(", ")}"
    logError(s"[Driver][InferSchema] $errMsg", lastErr)
    throw new RuntimeException(errMsg, lastErr)
  }
}

/**
 * Spark DataSource V2 `ScanBuilder` with column pruning (`SupportsPushDownRequiredColumns`)
 * and primary-key filter pushdown (`SupportsPushDownFilters`).
 */
class AerospikeFlightScanBuilder(
    fullTableSchema: StructType,
    options: AerospikeFlightOptions
) extends ScanBuilder
    with SupportsPushDownRequiredColumns
    with SupportsPushDownFilters
    with Logging {

  private var requiredSchema: StructType = fullTableSchema
  private var acceptedFilters: Array[Filter] = Array.empty
  private var pushedKeyLiterals: Option[Seq[KeyLiteralSpec]] = None

  private val keyField: StructField = fullTableSchema.fields
    .find(_.name == options.keyColumn)
    .getOrElse(StructField(options.keyColumn, StringType, nullable = false))

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
    logInfo(
      s"[Driver][ScanPlan] Column pruning applied on '${options.namespace}.${options.setName}': " +
        s"requiredColumns=[${requiredSchema.fields.map(_.name).mkString(", ")}] " +
        s"(${requiredSchema.fields.length}/${fullTableSchema.fields.length} columns)"
    )
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val (pkFilters, otherFilters) = filters.partition(isPkFilter)
    if (pkFilters.nonEmpty) {
      val extractedSets = pkFilters.flatMap(extractKeysFromFilter)
      if (extractedSets.length == pkFilters.length) {
        // Intersect all conjunctive PK filters
        val combined = extractedSets.reduceLeft((a, b) => a.filter(b.contains)).distinct
        this.pushedKeyLiterals = Some(combined)
        this.acceptedFilters = pkFilters
        logInfo(
          s"[Driver][ScanPlan] Pushed PK filter(s) on '${options.namespace}.${options.setName}' " +
            s"(keyColumn='${options.keyColumn}'): pushed=[${pkFilters.mkString(", ")}] -> " +
            s"extracted ${combined.size} key(s) for direct BatchGet; postScanFilters=[${otherFilters.mkString(", ")}]"
        )
        return otherFilters
      }
    }
    filters
  }

  override def pushedFilters(): Array[Filter] = acceptedFilters

  private def isPkFilter(f: Filter): Boolean = extractKeysFromFilter(f).isDefined

  private def extractKeysFromFilter(filter: Filter): Option[Seq[KeyLiteralSpec]] = {
    filter match {
      case EqualTo(attr, value) if attr == options.keyColumn =>
        KeyLiteralSpec.fromSparkValue(value, keyField.dataType).map(Seq(_))
      case EqualNullSafe(attr, value) if attr == options.keyColumn && value != null =>
        KeyLiteralSpec.fromSparkValue(value, keyField.dataType).map(Seq(_))
      case In(attr, values) if attr == options.keyColumn =>
        val parsed = values.flatMap(v => KeyLiteralSpec.fromSparkValue(v, keyField.dataType)).toSeq
        Some(parsed)
      case Or(left, right) =>
        for {
          lKeys <- extractKeysFromFilter(left)
          rKeys <- extractKeysFromFilter(right)
        } yield (lKeys ++ rKeys).distinct
      case And(left, right) =>
        for {
          lKeys <- extractKeysFromFilter(left)
          rKeys <- extractKeysFromFilter(right)
        } yield lKeys.filter(rKeys.contains)
      case _ =>
        None
    }
  }

  override def build(): Scan = {
    val explicitOptionKeys = options.readKeys.map(raw => KeyLiteralSpec.parseOptionKeys(raw, keyField.dataType))
    val effectiveKeys: Option[Seq[KeyLiteralSpec]] = (explicitOptionKeys, pushedKeyLiterals) match {
      case (Some(optK), Some(pushK)) => Some(optK.filter(pushK.contains))
      case (Some(optK), None)        => Some(optK)
      case (None, Some(pushK))       => Some(pushK)
      case (None, None)              => None
    }

    new AerospikeFlightScan(
      readSchema = requiredSchema,
      keyField = keyField,
      options = options,
      batchKeysOpt = effectiveKeys
    )
  }
}

sealed trait AerospikeFlightInputPartition extends InputPartition {
  def partitionIndex: Int
}

case class AerospikePartitionRangeInputPartition(
    partitionIndex: Int,
    beginPartition: Int,
    partitionCount: Int
) extends AerospikeFlightInputPartition

case class AerospikeBatchKeysInputPartition(
    partitionIndex: Int,
    keys: Seq[KeyLiteralSpec]
) extends AerospikeFlightInputPartition

case class AerospikeLookupViewInputPartition(
    partitionIndex: Int,
    inputSchema: StructType,
    binsOnlySchema: StructType,
    outputSchema: StructType,
    rows: Array[InternalRow]
) extends AerospikeFlightInputPartition

class AerospikeFlightScan(
    val readSchema: StructType,
    val keyField: StructField,
    val options: AerospikeFlightOptions,
    val batchKeysOpt: Option[Seq[KeyLiteralSpec]]
) extends Scan
    with Batch
    with Logging {

  override def description(): String = {
    if (options.lookupSourceView.isDefined) {
      s"AerospikeFlightDoExchangeLookupScan(${options.namespace}.${options.setName}, view=${options.lookupSourceView.get})"
    } else {
      batchKeysOpt match {
        case Some(keys) =>
          s"AerospikeFlightBatchGetScan(${options.namespace}.${options.setName}, keys=${keys.size})"
        case None =>
          s"AerospikeFlightPartitionScan(${options.namespace}.${options.setName}, partitions=${options.readNumPartitions})"
      }
    }
  }

  override def supportedCustomMetrics(): Array[CustomMetric] =
    AerospikeCustomMetrics.supportedReadMetrics

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    options.lookupSourceView match {
      case Some(viewName) =>
        val spark = org.apache.spark.sql.SparkSession.active
        val sourceDf = spark.table(viewName)
        val inputSchema = sourceDf.schema
        val binsFields = readSchema.fields.filterNot(f => inputSchema.fields.exists(_.name == f.name))
        val binsOnlySchema = StructType(binsFields)
        val fullOutputSchema = AerospikeFlightLookup.buildOutputSchema(
          inputSchema,
          binsOnlySchema,
          options.keyColumn,
          options.lookupJoinType
        )
        val maxViewRows = options.maxLookupViewRows
        val partitionRows: Array[Array[InternalRow]] = sourceDf.queryExecution.toRdd
          .mapPartitions { iter =>
            val proj = org.apache.spark.sql.catalyst.expressions.UnsafeProjection.create(inputSchema)
            val buf = new ArrayBuffer[InternalRow]()
            var count = 0L
            while (iter.hasNext) {
              count += 1L
              if (count > maxViewRows) {
                throw new IllegalArgumentException(
                  s"Lookup source view '$viewName' exceeded 'aerospike.lookup.maxViewRows' ($maxViewRows) in a single partition. " +
                    "Use AerospikeFlightLookup.lookup(keysDf, ...) / AerospikeFlightLookup.enrich(...) for zero-driver-collection distributed MPP lookups, " +
                    "or increase 'aerospike.lookup.maxViewRows'."
                )
              }
              buf += proj(iter.next()).copy().asInstanceOf[InternalRow]
            }
            Iterator.single(buf.toArray)
          }
          .collect()

        var totalCollectedRows = 0L
        var pIdx = 0
        while (pIdx < partitionRows.length) {
          totalCollectedRows += partitionRows(pIdx).length.toLong
          if (totalCollectedRows > maxViewRows) {
            throw new IllegalArgumentException(
              s"Lookup source view '$viewName' has more than 'aerospike.lookup.maxViewRows' ($maxViewRows) total rows. " +
                "Use AerospikeFlightLookup.lookup(keysDf, ...) / AerospikeFlightLookup.enrich(...) for zero-driver-collection distributed MPP lookups, " +
                "or increase 'aerospike.lookup.maxViewRows'."
            )
          }
          pIdx += 1
        }

        logInfo(
          s"[Driver][ScanPlan] Planned DoExchange Lookup scan on '${options.namespace}.${options.setName}' " +
            s"from view='$viewName': partitions=${partitionRows.length}, totalRows=$totalCollectedRows, " +
            s"joinType='${options.lookupJoinType}', lookupBins=[${binsFields.map(_.name).mkString(", ")}]"
        )

        partitionRows.zipWithIndex.map { case (rows, idx) =>
          AerospikeLookupViewInputPartition(
            partitionIndex = idx,
            inputSchema = inputSchema,
            binsOnlySchema = binsOnlySchema,
            outputSchema = fullOutputSchema,
            rows = rows
          ): InputPartition
        }

      case None =>
        batchKeysOpt match {
          case Some(keys) =>
            if (keys.isEmpty) {
              logInfo(
                s"[Driver][ScanPlan] Planned BatchKeys scan on '${options.namespace}.${options.setName}' with 0 keys (empty result)"
              )
              Array.empty[InputPartition]
            } else {
              val numSplits = math.min(options.readNumPartitions, keys.size).max(1)
              val chunkSize = (keys.size + numSplits - 1) / numSplits
              val parts = keys
                .grouped(chunkSize)
                .zipWithIndex
                .map { case (chunk, idx) =>
                  AerospikeBatchKeysInputPartition(idx, chunk): InputPartition
                }
                .toArray
              logInfo(
                s"[Driver][ScanPlan] Planned direct BatchKeys scan on '${options.namespace}.${options.setName}': " +
                  s"totalKeys=${keys.size}, sparkPartitions=${parts.length}, readSchema=[${readSchema.fields.map(_.name).mkString(", ")}]"
              )
              parts
            }

          case None =>
            val totalAerospikePartitions = 4096
            val numSplits = math.min(totalAerospikePartitions, math.max(1, options.readNumPartitions))
            val baseCount = totalAerospikePartitions / numSplits
            val remainder = totalAerospikePartitions % numSplits

            val partitions = new Array[InputPartition](numSplits)
            var currentBegin = 0
            var i = 0
            while (i < numSplits) {
              val count = baseCount + (if (i < remainder) 1 else 0)
              partitions(i) = AerospikePartitionRangeInputPartition(
                partitionIndex = i,
                beginPartition = currentBegin,
                partitionCount = count
              )
              currentBegin += count
              i += 1
            }
            logInfo(
              s"[Driver][ScanPlan] Planned full 4096-partition scan on '${options.namespace}.${options.setName}': " +
                s"sparkPartitions=$numSplits (~$baseCount Aerospike partitions/task), " +
                s"readBatchSize=${options.readBatchSize}, readSchema=[${readSchema.fields.map(_.name).mkString(", ")}]"
            )
            partitions
        }
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new AerospikeFlightPartitionReaderFactory(readSchema, keyField, options)
  }
}

class AerospikeFlightPartitionReaderFactory(
    readSchema: StructType,
    keyField: StructField,
    options: AerospikeFlightOptions
) extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    partition.asInstanceOf[AerospikeFlightInputPartition] match {
      case lookupPart: AerospikeLookupViewInputPartition =>
        new AerospikeFlightLookupPartitionReader(lookupPart, readSchema, options)
      case scanPart =>
        new AerospikeFlightPartitionReader(scanPart, readSchema, keyField, options)
    }
  }
}

class AerospikeFlightLookupPartitionReader(
    partition: AerospikeLookupViewInputPartition,
    readSchema: StructType,
    options: AerospikeFlightOptions
) extends PartitionReader[InternalRow] {

  private val iter: AerospikeFlightLookup.CloseableRowIterator =
    AerospikeFlightLookup.enrichPartitionIteratorInternal(
      partitionIndex = partition.partitionIndex,
      inputRows = partition.rows.iterator,
      inputSchema = partition.inputSchema,
      binsOnlySchema = partition.binsOnlySchema,
      outputSchema = readSchema,
      options = options,
      copyRows = false
    )

  private var currentRow: InternalRow = _

  override def next(): Boolean = {
    if (iter.hasNext) {
      currentRow = iter.next()
      true
    } else {
      false
    }
  }

  override def get(): InternalRow = currentRow

  override def close(): Unit = {
    iter.close()
  }
}

class AerospikeFlightPartitionReader(
    partition: AerospikeFlightInputPartition,
    readSchema: StructType,
    keyField: StructField,
    options: AerospikeFlightOptions
) extends PartitionReader[InternalRow]
    with Logging {

  private val startTimeMs: Long = System.currentTimeMillis()
  private val allocator: BufferAllocator = new RootAllocator(Long.MaxValue)

  // Ensure wireSchema has at least the key column so Arrow VectorSchemaRoot always has >= 1 vector
  private val wireSchema: StructType = {
    if (readSchema.fields.exists(_.name == options.keyColumn)) {
      readSchema
    } else {
      StructType(keyField +: readSchema.fields)
    }
  }

  private val requestedBins: Seq[String] =
    readSchema.fields.map(_.name).filterNot(_ == options.keyColumn).toSeq

  private val modeDesc: String = partition match {
    case AerospikePartitionRangeInputPartition(_, begin, count) =>
      s"PartitionRange(begin=$begin, count=$count)"
    case AerospikeBatchKeysInputPartition(_, keys) =>
      s"BatchKeys(count=${keys.size})"
    case _ =>
      "LookupView"
  }

  private var connectedEndpointStr: String = "unconnected"
  private var flightClient: FlightClient = _
  private var flightStream: FlightStream = _
  private var rowReader: ArrowRowReader = _
  private var currentBatchRowIdx: Int = 0
  private var currentBatchRowCount: Int = 0
  private var rowsReadCount: Long = 0L
  private var batchesReadCount: Long = 0L

  initializeStream()

  private def initializeStream(): Unit = {
    val ticketBytes = buildTicketJsonBytes()
    val ticket = new Ticket(ticketBytes)

    var lastErr: Throwable = null
    var attempt = 0
    while (attempt < options.hosts.size) {
      val endpoint = options.endpointFor(partition.partitionIndex, attempt)
      try {
        flightClient = AerospikeLoadStatus.createFlightClient(allocator, endpoint, options)
        val callOptions = AerospikeLoadStatus.buildCallOptions(flightClient, options)
        flightStream = flightClient.getStream(ticket, callOptions: _*)
        FlightCompressionHelper.enableStreamDecompression(flightStream)
        val root = flightStream.getRoot
        rowReader = new ArrowRowReader(readSchema, root)
        connectedEndpointStr = s"${endpoint.host}:${endpoint.port}"
        logInfo(
          s"[Executor][Read] Opened DoGet stream for partition=${partition.partitionIndex} ($modeDesc) " +
            s"on endpoint=$connectedEndpointStr, target='${options.namespace}.${options.setName}', " +
            s"compression='${options.compression}', requestedBins=[${requestedBins.mkString(", ")}]"
        )
        return
      } catch {
        case t: Throwable =>
          lastErr = t
          logWarning(
            s"[Executor][Read] Failed to open DoGet stream for partition=${partition.partitionIndex} ($modeDesc) " +
              s"on ${endpoint.host}:${endpoint.port} (attempt ${attempt + 1}/${options.hosts.size}): ${t.getMessage}"
          )
          closeClientAndStream()
          attempt += 1
      }
    }
    val errMsg =
      s"Failed to open Arrow Flight DoGet stream for partition ${partition.partitionIndex} ($modeDesc) across endpoints ${options.hosts.map(h => s"${h.host}:${h.port}").mkString(", ")}"
    logError(s"[Executor][Read] $errMsg", lastErr)
    throw new RuntimeException(errMsg, lastErr)
  }

  private def buildTicketJsonBytes(): Array[Byte] = {
    val map = new util.LinkedHashMap[String, Any]()
    map.put("namespace", options.namespace)
    map.put("set_name", options.setName)
    map.put("key_column", options.keyColumn)
    map.put("bins", requestedBins.asJava)
    map.put("batch_size", Int.box(options.readBatchSize))
    map.put("type_mismatch_mode", options.readTypeMismatchMode)
    map.put("schema_ipc_base64", ArrowSchemaConverter.toArrowSchemaIpcBase64(wireSchema))
    map.put("compression", options.compression)
    map.put("compression_level", Int.box(options.compressionLevel))

    val readModeMap = new util.LinkedHashMap[String, Any]()
    partition match {
      case AerospikePartitionRangeInputPartition(_, begin, count) =>
        readModeMap.put("mode", "partition_range")
        readModeMap.put("begin_partition", Int.box(begin))
        readModeMap.put("partition_count", Int.box(count))
      case AerospikeBatchKeysInputPartition(_, keys) =>
        readModeMap.put("mode", "batch_keys")
        readModeMap.put("keys", keys.map(_.toMap).asJava)
      case _: AerospikeLookupViewInputPartition =>
        throw new IllegalStateException("AerospikeLookupViewInputPartition uses AerospikeFlightLookupPartitionReader")
    }
    map.put("read_mode", readModeMap)
    AerospikeFlightOptions.SharedMapper.writeValueAsBytes(map)
  }

  override def next(): Boolean = {
    while (currentBatchRowIdx >= currentBatchRowCount) {
      if (flightStream == null || !flightStream.next()) {
        return false
      }
      currentBatchRowIdx = 0
      currentBatchRowCount = flightStream.getRoot.getRowCount
      if (currentBatchRowCount > 0) {
        batchesReadCount += 1L
      }
    }
    true
  }

  override def get(): InternalRow = {
    val row = rowReader.readRow(currentBatchRowIdx)
    currentBatchRowIdx += 1
    rowsReadCount += 1L
    row
  }

  override def currentMetricsValues(): Array[CustomTaskMetric] = {
    Array(
      new CustomTaskMetric {
        override def name(): String = "as_rows_read"
        override def value(): Long = rowsReadCount
      },
      new CustomTaskMetric {
        override def name(): String = "as_batches_read"
        override def value(): Long = batchesReadCount
      }
    )
  }

  private def closeClientAndStream(): Unit = {
    if (flightStream != null) {
      try { flightStream.close() } catch { case _: Throwable => }
      flightStream = null
    }
    if (flightClient != null) {
      try { flightClient.close() } catch { case _: Throwable => }
      flightClient = null
    }
  }

  override def close(): Unit = {
    val elapsedMs = math.max(0L, System.currentTimeMillis() - startTimeMs)
    logInfo(
      s"[Executor][Read] Closed DoGet reader for partition=${partition.partitionIndex} ($modeDesc) " +
        s"on $connectedEndpointStr: rowsRead=$rowsReadCount, batchesRead=$batchesReadCount, elapsedMs=$elapsedMs"
    )
    closeClientAndStream()
    try { allocator.close() } catch { case _: Throwable => }
  }
}
