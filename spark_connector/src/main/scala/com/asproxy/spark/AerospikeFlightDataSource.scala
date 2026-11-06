package com.asproxy.spark

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{
  SupportsRead,
  SupportsWrite,
  Table,
  TableCapability,
  TableProvider
}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable

object AerospikeFlightStatusSchema {
  val schema: StructType = new StructType()
    .add("job_id", StringType, nullable = false)
    .add("state", StringType, nullable = false)
    .add("namespace", StringType, nullable = true)
    .add("set_name", StringType, nullable = true)
    .add("rows_written", LongType, nullable = false)
    .add("batches_processed", LongType, nullable = false)
    .add("overload_retries", LongType, nullable = false)
    .add("other_retries", LongType, nullable = false)
    .add("elapsed_ms", LongType, nullable = false)
    .add("raw_json", StringType, nullable = false)
}

/**
 * Spark 3.5 DataSource V2 entry point for:
 *  1. Writing DataFrames to Aerospike via the Rust Arrow Flight MPP Proxy (`df.write.format("aerospike-flight")`).
 *  2. Reading load job statistics from the Raft proxy cluster or driver registry (`spark.read.format("aerospike-flight")`),
 *     making status retrieval native in both PySpark and Scala.
 */
class AerospikeFlightDataSource extends TableProvider with DataSourceRegister {

  override def shortName(): String = "aerospike-flight"

  override def supportsExternalMetadata(): Boolean = true

  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    AerospikeFlightStatusSchema.schema

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]
  ): Table = {
    val rawMap = properties.asScala.toMap
    val hasExplicitJobId = rawMap.keys.exists(k =>
      k.equalsIgnoreCase("connector.jobId") || k.equalsIgnoreCase("job.id")
    )
    val options = AerospikeFlightOptions.forStatusQuery(rawMap)
    new AerospikeFlightTable(schema, options, hasExplicitJobId)
  }
}

class AerospikeFlightTable(
    val tableSchema: StructType,
    val options: AerospikeFlightOptions,
    val hasExplicitJobId: Boolean
) extends Table
    with SupportsWrite
    with SupportsRead {

  override def name(): String = s"aerospike-flight:${options.namespace}.${options.setName}"

  override def schema(): StructType =
    if (tableSchema != null && tableSchema.nonEmpty) tableSchema
    else AerospikeFlightStatusSchema.schema

  override def capabilities(): util.Set[TableCapability] = {
    Set(
      TableCapability.BATCH_READ,
      TableCapability.BATCH_WRITE,
      TableCapability.STREAMING_WRITE,
      TableCapability.ACCEPT_ANY_SCHEMA
    ).asJava
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    options.validateWriteTarget()
    new AerospikeFlightWriteBuilder(info.schema(), options)
  }

  override def newScanBuilder(scanOptions: CaseInsensitiveStringMap): ScanBuilder = {
    val rawScanMap = scanOptions.asCaseSensitiveMap().asScala.toMap
    val explicitJob = hasExplicitJobId || rawScanMap.keys.exists(k =>
      k.equalsIgnoreCase("connector.jobId") || k.equalsIgnoreCase("job.id")
    )
    val mergedOpts =
      if (rawScanMap.nonEmpty) AerospikeFlightOptions.forStatusQuery(rawScanMap)
      else options
    new AerospikeFlightStatusScanBuilder(mergedOpts, explicitJob)
  }
}

case class StatusRowData(
    jobId: String,
    state: String,
    namespace: String,
    setName: String,
    rowsWritten: Long,
    batchesProcessed: Long,
    overloadRetries: Long,
    otherRetries: Long,
    elapsedMs: Long,
    rawJson: String
) extends Serializable

case class AerospikeStatusInputPartition(rows: Seq[StatusRowData]) extends InputPartition

class AerospikeFlightStatusScanBuilder(
    options: AerospikeFlightOptions,
    hasExplicitJobId: Boolean
) extends ScanBuilder
    with Scan
    with Batch {

  override def build(): Scan = this

  override def readSchema(): StructType = AerospikeFlightStatusSchema.schema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    val rows = AerospikeFlightStatusScanBuilder.collectStatusRows(options, hasExplicitJobId)
    Array(AerospikeStatusInputPartition(rows))
  }

  override def createReaderFactory(): PartitionReaderFactory =
    new AerospikeStatusReaderFactory()
}

object AerospikeFlightStatusScanBuilder {
  def collectStatusRows(
      options: AerospikeFlightOptions,
      hasExplicitJobId: Boolean
  ): Seq[StatusRowData] = {
    val mapper = new ObjectMapper()
    val results = mutable.LinkedHashMap.empty[String, StatusRowData]

    if (hasExplicitJobId) {
      val localOpt = AerospikeLoadStatus.getSummary(options.jobId)
      localOpt.foreach { s =>
        results.put(s.jobId, fromLocalSummary(s))
      }

      try {
        val remoteJson = AerospikeLoadStatus.fetchRemoteJobStatus(options, options.jobId)
        val node = mapper.readTree(remoteJson)
        if (node != null && node.has("job_id")) {
          val row = fromRaftJobJson(node, remoteJson, localOpt.map(_.elapsedMs))
          results.put(row.jobId, row)
        }
      } catch {
        case _: Throwable => // Fallback to local driver registry if proxy is unreachable
      }
    } else {
      AerospikeLoadStatus.allSummaries.foreach { s =>
        results.put(s.jobId, fromLocalSummary(s))
      }

      try {
        val clusterJson = AerospikeLoadStatus.fetchClusterStatus(options)
        val root = mapper.readTree(clusterJson)
        if (root != null && root.has("state") && root.get("state").has("jobs")) {
          val jobsNode = root.get("state").get("jobs")
          val fields = jobsNode.fields().asScala
          for (entry <- fields) {
            val jobNode = entry.getValue
            val rawJobJson = mapper.writeValueAsString(jobNode)
            val localElapsed = AerospikeLoadStatus.getSummary(entry.getKey).map(_.elapsedMs)
            val row = fromRaftJobJson(jobNode, rawJobJson, localElapsed)
            results.put(row.jobId, row)
          }
        }
      } catch {
        case _: Throwable => // Fallback to local driver registry
      }
    }

    results.values.toSeq
  }

  private def fromLocalSummary(s: LoadJobSummary): StatusRowData = {
    StatusRowData(
      jobId = s.jobId,
      state = s.status.toLowerCase,
      namespace = s.namespace,
      setName = s.setName,
      rowsWritten = s.rowsWritten,
      batchesProcessed = s.batchesSent,
      overloadRetries = s.overloadRetries,
      otherRetries = s.otherRetries,
      elapsedMs = s.elapsedMs,
      rawJson = s.toJson
    )
  }

  private def fromRaftJobJson(
      node: JsonNode,
      rawJson: String,
      localElapsedMs: Option[Long]
  ): StatusRowData = {
    val jobId = if (node.has("job_id")) node.get("job_id").asText("") else ""
    val state = if (node.has("state")) node.get("state").asText("running") else "running"
    val ns = if (node.has("namespace")) node.get("namespace").asText("") else ""
    val set = if (node.has("set_name")) node.get("set_name").asText("") else ""
    val rows = if (node.has("total_rows_written")) node.get("total_rows_written").asLong(0L) else 0L
    val batches =
      if (node.has("total_batches_processed")) node.get("total_batches_processed").asLong(0L)
      else 0L
    val overload =
      if (node.has("total_overload_retries")) node.get("total_overload_retries").asLong(0L) else 0L
    val other =
      if (node.has("total_other_retries")) node.get("total_other_retries").asLong(0L) else 0L
    val startedAt = if (node.has("started_at_ms")) node.get("started_at_ms").asLong(0L) else 0L
    val updatedAt = if (node.has("updated_at_ms")) node.get("updated_at_ms").asLong(0L) else 0L
    val raftElapsed = math.max(0L, updatedAt - startedAt)

    StatusRowData(
      jobId = jobId,
      state = state,
      namespace = ns,
      setName = set,
      rowsWritten = rows,
      batchesProcessed = batches,
      overloadRetries = overload,
      otherRetries = other,
      elapsedMs = localElapsedMs.getOrElse(raftElapsed),
      rawJson = rawJson
    )
  }
}

class AerospikeStatusReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val statusPart = partition.asInstanceOf[AerospikeStatusInputPartition]
    new PartitionReader[InternalRow] {
      private val iter = statusPart.rows.iterator
      private var currentRow: InternalRow = _

      override def next(): Boolean = {
        if (iter.hasNext) {
          val r = iter.next()
          currentRow = InternalRow(
            UTF8String.fromString(r.jobId),
            UTF8String.fromString(r.state),
            UTF8String.fromString(r.namespace),
            UTF8String.fromString(r.setName),
            r.rowsWritten,
            r.batchesProcessed,
            r.overloadRetries,
            r.otherRetries,
            r.elapsedMs,
            UTF8String.fromString(r.rawJson)
          )
          true
        } else {
          false
        }
      }

      override def get(): InternalRow = currentRow

      override def close(): Unit = {}
    }
  }
}
