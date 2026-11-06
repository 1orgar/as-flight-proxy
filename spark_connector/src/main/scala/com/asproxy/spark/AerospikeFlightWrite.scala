package com.asproxy.spark

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.types.StructType

/**
 * Spark 3.5 DataSource V2 WriteBuilder, BatchWrite, StreamingWrite, and DataWriterFactory.
 */
class AerospikeFlightWriteBuilder(
    schema: StructType,
    options: AerospikeFlightOptions
) extends WriteBuilder
    with SupportsTruncate {

  override def truncate(): WriteBuilder = this

  override def build(): Write = new Write {
    override def toBatch: BatchWrite = new AerospikeFlightBatchWrite(schema, options)
    override def toStreaming: StreamingWrite = new AerospikeFlightStreamingWrite(schema, options)
    override def supportedCustomMetrics(): Array[CustomMetric] = AerospikeCustomMetrics.supportedMetrics
  }
}

class AerospikeFlightBatchWrite(
    schema: StructType,
    options: AerospikeFlightOptions
) extends BatchWrite {

  private val startTimeMs: Long = System.currentTimeMillis()

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    new AerospikeFlightWriterFactory(schema, options)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    val commitMsgs = messages.collect { case m: AerospikeFlightCommitMessage => m }
    val raftStatusJson = AerospikeLoadStatus.notifyJobState(options, "complete_job")
    val summary = AerospikeFlightCommitMessage.aggregate(
      options = options,
      status = "COMPLETED",
      messages = commitMsgs,
      elapsedMs = math.max(0L, System.currentTimeMillis() - startTimeMs),
      raftClusterStatusJson = raftStatusJson
    )
    AerospikeLoadStatus.recordSummary(summary)
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    val commitMsgs = messages.collect { case m: AerospikeFlightCommitMessage => m }
    val raftStatusJson =
      AerospikeLoadStatus.notifyJobState(options, "fail_job", Some("Spark job aborted"))
    val summary = AerospikeFlightCommitMessage.aggregate(
      options = options,
      status = "FAILED",
      messages = commitMsgs,
      elapsedMs = math.max(0L, System.currentTimeMillis() - startTimeMs),
      raftClusterStatusJson = raftStatusJson
    )
    AerospikeLoadStatus.recordSummary(summary)
  }
}

class AerospikeFlightStreamingWrite(
    schema: StructType,
    options: AerospikeFlightOptions
) extends StreamingWrite {

  override def createStreamingWriterFactory(info: PhysicalWriteInfo): StreamingDataWriterFactory = {
    new AerospikeFlightWriterFactory(schema, options)
  }

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    val commitMsgs = messages.collect { case m: AerospikeFlightCommitMessage => m }
    val raftStatusJson = AerospikeLoadStatus.notifyJobState(options, "complete_job")
    val summary = AerospikeFlightCommitMessage.aggregate(
      options = options,
      status = "COMPLETED",
      messages = commitMsgs,
      elapsedMs = commitMsgs.map(_.elapsedMs).foldLeft(0L)(math.max),
      raftClusterStatusJson = raftStatusJson
    )
    AerospikeLoadStatus.recordSummary(summary)
  }

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    val commitMsgs = messages.collect { case m: AerospikeFlightCommitMessage => m }
    val raftStatusJson = AerospikeLoadStatus.notifyJobState(
      options,
      "fail_job",
      Some(s"Spark streaming epoch $epochId aborted")
    )
    val summary = AerospikeFlightCommitMessage.aggregate(
      options = options,
      status = "FAILED",
      messages = commitMsgs,
      elapsedMs = commitMsgs.map(_.elapsedMs).foldLeft(0L)(math.max),
      raftClusterStatusJson = raftStatusJson
    )
    AerospikeLoadStatus.recordSummary(summary)
  }
}

class AerospikeFlightWriterFactory(
    schema: StructType,
    options: AerospikeFlightOptions
) extends DataWriterFactory
    with StreamingDataWriterFactory
    with Serializable {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new AerospikeFlightDataWriter(partitionId, taskId, schema, options)
  }

  override def createWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long
  ): DataWriter[InternalRow] = {
    new AerospikeFlightDataWriter(partitionId, taskId, schema, options)
  }
}

case class AerospikeFlightCommitMessage(
    partitionId: Int,
    taskId: Long,
    rowsWritten: Long,
    batchesSent: Long,
    overloadRetries: Long,
    otherRetries: Long,
    elapsedMs: Long,
    rowsValidated: Long = 0L,
    matchedRows: Long = 0L,
    mismatchedRows: Long = 0L,
    missingKeys: Long = 0L,
    binMismatchRows: Long = 0L
) extends WriterCommitMessage

object AerospikeFlightCommitMessage {
  def aggregate(
      options: AerospikeFlightOptions,
      status: String,
      messages: Seq[AerospikeFlightCommitMessage],
      elapsedMs: Long,
      raftClusterStatusJson: Option[String] = None
  ): LoadJobSummary = {
    val totalValidated = messages.map(_.rowsValidated).sum
    val totalMatched = messages.map(_.matchedRows).sum
    val totalMismatched = messages.map(_.mismatchedRows).sum
    val totalMissing = messages.map(_.missingKeys).sum
    val totalBinMismatch = messages.map(_.binMismatchRows).sum

    LoadJobSummary(
      jobId = options.jobId,
      namespace = options.namespace,
      setName = options.setName,
      status = status,
      rowsWritten = messages.map(_.rowsWritten).sum,
      batchesSent = messages.map(_.batchesSent).sum,
      overloadRetries = messages.map(_.overloadRetries).sum,
      otherRetries = messages.map(_.otherRetries).sum,
      partitionsCount = messages.size,
      elapsedMs = elapsedMs,
      raftClusterStatusJson = raftClusterStatusJson,
      validateOnly = options.validateOnly,
      rowsValidated = totalValidated,
      matchedRows = totalMatched,
      mismatchedRows = totalMismatched,
      missingKeys = totalMissing,
      binMismatchRows = totalBinMismatch,
      dqPassed = if (options.validateOnly) Some(totalMismatched == 0L) else None
    )
  }
}
