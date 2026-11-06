package com.asproxy.spark

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.arrow.flight._
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

/**
 * Multi-threaded Spark DataWriter that converts `InternalRow` partitions into Apache Arrow
 * `RecordBatch`es and streams them in parallel over Arrow Flight `DoPut` to `proxy_server`.
 */
class AerospikeFlightDataWriter(
    partitionId: Int,
    taskId: Long,
    sparkSchema: StructType,
    options: AerospikeFlightOptions
) extends DataWriter[InternalRow] {

  private val startTimeMs: Long = System.currentTimeMillis()
  private val rootAllocator: BufferAllocator = new RootAllocator(Long.MaxValue)
  private val arrowSchema = ArrowSchemaConverter.toArrowSchema(sparkSchema)
  private val stagingRoot: VectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, rootAllocator)
  private val batchConverter = new ArrowBatchConverter(sparkSchema, options.keyColumn, stagingRoot)

  private val batchQueue = new ArrayBlockingQueue[Option[ArrowRecordBatch]](options.queueCapacity)
  private val workerError = new AtomicReference[Throwable](null)
  private val rowsWrittenCounter = new AtomicLong(0L)
  private val batchesSentCounter = new AtomicLong(0L)
  private val overloadRetriesCounter = new AtomicLong(0L)
  private val otherRetriesCounter = new AtomicLong(0L)

  private val executor: ExecutorService = Executors.newFixedThreadPool(
    options.writerThreads,
    new ThreadFactory {
      private val idx = new AtomicLong(0)
      override def newThread(r: Runnable): Thread = {
        val t = new Thread(r, s"as-flight-writer-p$partitionId-t$taskId-w${idx.getAndIncrement()}")
        t.setDaemon(true)
        t
      }
    }
  )

  private val workerFutures: Seq[Future[_]] = (0 until options.writerThreads).map { threadIdx =>
    executor.submit(new Runnable {
      override def run(): Unit = runSenderLoop(threadIdx)
    })
  }

  batchConverter.reset()

  override def write(record: InternalRow): Unit = {
    checkWorkerFailure()
    batchConverter.appendRow(record)
    if (batchConverter.rowCount >= options.batchSize) {
      flushCurrentBatch()
    }
  }

  override def commit(): WriterCommitMessage = {
    try {
      checkWorkerFailure()
      if (batchConverter.rowCount > 0) {
        flushCurrentBatch()
      }

      // Send poison pills (None) to all sender threads
      var i = 0
      while (i < options.writerThreads) {
        enqueueWithFailureCheck(None)
        i += 1
      }

      // Wait for all sender threads to finish streaming and receiving PutResult acks
      workerFutures.foreach { fut =>
        try {
          fut.get()
        } catch {
          case e: ExecutionException =>
            throw new RuntimeException("Arrow Flight sender worker failed", e.getCause)
        }
      }
      checkWorkerFailure()

      AerospikeFlightCommitMessage(
        partitionId = partitionId,
        taskId = taskId,
        rowsWritten = rowsWrittenCounter.get(),
        batchesSent = batchesSentCounter.get(),
        overloadRetries = overloadRetriesCounter.get(),
        otherRetries = otherRetriesCounter.get(),
        elapsedMs = math.max(0L, System.currentTimeMillis() - startTimeMs)
      )
    } finally {
      closeResources()
    }
  }

  override def abort(): Unit = {
    try {
      executor.shutdownNow()
      drainQueue()
    } finally {
      closeResources()
    }
  }

  override def close(): Unit = {
    closeResources()
  }

  override def currentMetricsValues(): Array[CustomTaskMetric] = {
    Array(
      createTaskMetric("as_rows_written", rowsWrittenCounter.get()),
      createTaskMetric("as_batches_sent", batchesSentCounter.get()),
      createTaskMetric("as_overload_retries", overloadRetriesCounter.get()),
      createTaskMetric("as_other_retries", otherRetriesCounter.get())
    )
  }

  private def createTaskMetric(metricName: String, metricValue: Long): CustomTaskMetric = {
    new CustomTaskMetric {
      override def name(): String = metricName
      override def value(): Long = metricValue
    }
  }

  private def flushCurrentBatch(): Unit = {
    val count = batchConverter.finishBatch()
    if (count > 0) {
      val unloader = new VectorUnloader(stagingRoot)
      val recordBatch = unloader.getRecordBatch
      enqueueWithFailureCheck(Some(recordBatch))
      rowsWrittenCounter.addAndGet(count.toLong)
    }
    batchConverter.reset()
  }

  private def enqueueWithFailureCheck(item: Option[ArrowRecordBatch]): Unit = {
    var offered = false
    while (!offered) {
      checkWorkerFailure()
      offered = batchQueue.offer(item, 200, TimeUnit.MILLISECONDS)
    }
  }

  private def checkWorkerFailure(): Unit = {
    val err = workerError.get()
    if (err != null) {
      throw new RuntimeException(
        s"Arrow Flight background writer failed in partition $partitionId (task $taskId): ${err.getMessage}",
        err
      )
    }
  }

  private def runSenderLoop(threadIdx: Int): Unit = {
    val mapper = new ObjectMapper()
    val workerAllocator = rootAllocator.newChildAllocator(
      s"worker-$threadIdx",
      0L,
      Long.MaxValue
    )
    var workerRoot: VectorSchemaRoot = null
    var flightClient: FlightClient = null

    try {
      workerRoot = VectorSchemaRoot.create(arrowSchema, workerAllocator)
      val loader = new VectorLoader(workerRoot)
      val endpoint = options.endpointFor(partitionId, threadIdx)
      flightClient = AerospikeLoadStatus.createFlightClient(workerAllocator, endpoint, options)

      val callOptions = AerospikeLoadStatus.buildCallOptions(flightClient, options)
      val descriptor = FlightDescriptor.command(options.toFlightDescriptorJsonBytes)

      val putListener = new SyncPutListener()
      val streamListener = flightClient.startPut(
        descriptor,
        workerRoot,
        putListener,
        callOptions: _*
      )

      try {
        var running = true
        while (running && workerError.get() == null) {
          val nextItem = batchQueue.poll(200, TimeUnit.MILLISECONDS)
          if (nextItem != null) {
            nextItem match {
              case Some(recordBatch) =>
                try {
                  loader.load(recordBatch)
                } finally {
                  recordBatch.close()
                }
                streamListener.putNext()
                // Read PutResult ack from proxy_server to confirm batch persistence in Aerospike
                val ack = putListener.read()
                if (ack != null) {
                  try {
                    val metaBuf = ack.getApplicationMetadata
                    if (metaBuf != null && metaBuf.readableBytes() > 0) {
                      val bytes = new Array[Byte](metaBuf.readableBytes())
                      metaBuf.getBytes(metaBuf.readerIndex(), bytes)
                      val jsonNode = mapper.readTree(bytes)
                      if (jsonNode != null) {
                        if (jsonNode.has("overload_retries")) {
                          overloadRetriesCounter.addAndGet(jsonNode.get("overload_retries").asLong(0L))
                        }
                        if (jsonNode.has("other_retries")) {
                          otherRetriesCounter.addAndGet(jsonNode.get("other_retries").asLong(0L))
                        }
                      }
                    }
                  } finally {
                    ack.close()
                  }
                }
                batchesSentCounter.incrementAndGet()

              case None =>
                running = false
            }
          }
        }

        streamListener.completed()
        putListener.getResult()
      } finally {
        putListener.close()
      }
    } catch {
      case t: Throwable =>
        workerError.compareAndSet(null, t)
        drainQueue()
    } finally {
      if (flightClient != null) {
        try { flightClient.close() } catch { case _: Throwable => }
      }
      if (workerRoot != null) {
        try { workerRoot.close() } catch { case _: Throwable => }
      }
      try { workerAllocator.close() } catch { case _: Throwable => }
    }
  }

  private def drainQueue(): Unit = {
    var item = batchQueue.poll()
    while (item != null) {
      item.foreach { rb =>
        try { rb.close() } catch { case _: Throwable => }
      }
      item = batchQueue.poll()
    }
  }

  private def closeResources(): Unit = {
    executor.shutdownNow()
    drainQueue()
    try { stagingRoot.close() } catch { case _: Throwable => }
    try { rootAllocator.close() } catch { case _: Throwable => }
  }
}
