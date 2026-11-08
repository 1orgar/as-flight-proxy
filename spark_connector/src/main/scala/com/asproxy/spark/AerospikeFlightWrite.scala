package com.asproxy.spark

import org.apache.spark.internal.Logging
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

  // Driver-side ФЛК (Schema, Bin Format & DataType Validation) on initialization
  AerospikeSchemaValidator.validate(schema, options)

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
) extends BatchWrite
    with Logging {

  private val startTimeMs: Long = System.currentTimeMillis()

  logInfo(
    s"[Driver][Write] Initialized batch write jobId='${options.jobId}', " +
      s"target='${options.namespace}.${options.setName}', writeMode='${options.writeMode}', " +
      s"validateOnly=${options.validateOnly}, batchSize=${options.batchSize}, " +
      s"writerThreads=${options.writerThreads}, hosts=[${options.hosts.map(h => s"${h.host}:${h.port}").mkString(", ")}]"
  )

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    logInfo(
      s"[Driver][Write] Creating BatchWriterFactory for jobId='${options.jobId}', numPartitions=${info.numPartitions()}"
    )
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
    val rps = if (summary.elapsedMs > 0) (summary.rowsWritten * 1000L) / summary.elapsedMs else summary.rowsWritten
    if (options.validateOnly) {
      logInfo(
        s"[Driver][DQ-Validate] Completed jobId='${summary.jobId}' on '${summary.namespace}.${summary.setName}' " +
          s"in ${summary.elapsedMs}ms (partitions=${summary.partitionsCount}): rowsValidated=${summary.rowsValidated}, " +
          s"matchedRows=${summary.matchedRows}, mismatchedRows=${summary.mismatchedRows}, " +
          s"missingKeys=${summary.missingKeys}, binMismatchRows=${summary.binMismatchRows}, " +
          s"dqPassed=${summary.dqPassed.getOrElse(false)}, overloadRetries=${summary.overloadRetries}, otherRetries=${summary.otherRetries}"
      )
    } else {
      logInfo(
        s"[Driver][Write] Completed jobId='${summary.jobId}' on '${summary.namespace}.${summary.setName}' " +
          s"in ${summary.elapsedMs}ms (~$rps rows/sec, partitions=${summary.partitionsCount}): " +
          s"rowsWritten=${summary.rowsWritten}, batchesSent=${summary.batchesSent}, " +
          s"overloadRetries=${summary.overloadRetries}, otherRetries=${summary.otherRetries}"
      )
    }
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
    logError(
      s"[Driver][Write] Aborted jobId='${summary.jobId}' on '${summary.namespace}.${summary.setName}' " +
        s"after ${summary.elapsedMs}ms (committedPartitions=${summary.partitionsCount}, " +
        s"rowsWritten=${summary.rowsWritten}, overloadRetries=${summary.overloadRetries}, otherRetries=${summary.otherRetries})"
    )
  }
}

class AerospikeFlightStreamingWrite(
    schema: StructType,
    options: AerospikeFlightOptions
) extends StreamingWrite
    with Logging {

  override def createStreamingWriterFactory(info: PhysicalWriteInfo): StreamingDataWriterFactory = {
    logInfo(
      s"[Driver][StreamingWrite] Creating StreamingWriterFactory for jobId='${options.jobId}', " +
        s"target='${options.namespace}.${options.setName}', numPartitions=${info.numPartitions()}"
    )
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
    logInfo(
      s"[Driver][StreamingWrite] Committed epochId=$epochId, jobId='${summary.jobId}' on '${summary.namespace}.${summary.setName}' " +
        s"in ${summary.elapsedMs}ms: rowsWritten=${summary.rowsWritten}, batchesSent=${summary.batchesSent}, " +
        s"overloadRetries=${summary.overloadRetries}, otherRetries=${summary.otherRetries}"
    )
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
    logError(
      s"[Driver][StreamingWrite] Aborted epochId=$epochId, jobId='${summary.jobId}' on '${summary.namespace}.${summary.setName}'"
    )
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
