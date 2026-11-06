package com.asproxy.spark

import org.apache.spark.sql.catalyst.InternalRow
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
  }
}

class AerospikeFlightBatchWrite(
    schema: StructType,
    options: AerospikeFlightOptions
) extends BatchWrite {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    new AerospikeFlightWriterFactory(schema, options)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}

class AerospikeFlightStreamingWrite(
    schema: StructType,
    options: AerospikeFlightOptions
) extends StreamingWrite {

  override def createStreamingWriterFactory(info: PhysicalWriteInfo): StreamingDataWriterFactory = {
    new AerospikeFlightWriterFactory(schema, options)
  }

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
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
    batchesSent: Long
) extends WriterCommitMessage
