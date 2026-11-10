package com.asproxy.spark

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.arrow.flight._
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.spark.internal.Logging
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
) extends DataWriter[InternalRow]
    with Logging {

  private val startTimeMs: Long = System.currentTimeMillis()
  private val rootAllocator: BufferAllocator = new RootAllocator(Long.MaxValue)
  private val arrowSchema = ArrowSchemaConverter.toArrowSchema(
    sparkSchema = sparkSchema,
    timeZoneId = "UTC",
    keyColumn = options.keyColumn,
    ignoreNulls = options.shouldOmitNullsInFlight,
    validateOnly = options.validateOnly
  )
  private val stagingRoot: VectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, rootAllocator)
  private val stagingUnloader: VectorUnloader = new VectorUnloader(stagingRoot)
  private val batchConverter = new ArrowBatchConverter(
    sparkSchema = sparkSchema,
    keyColumn = options.keyColumn,
    root = stagingRoot,
    ignoreNulls = options.shouldOmitNullsInFlight,
    validateOnly = options.validateOnly
  )

  private val batchQueue = new ArrayBlockingQueue[Option[ArrowRecordBatch]](options.queueCapacity)
  private val workerError = new AtomicReference[Throwable](null)
  private val rowsWrittenCounter = new AtomicLong(0L)
  private val batchesSentCounter = new AtomicLong(0L)
  private val overloadRetriesCounter = new AtomicLong(0L)
  private val otherRetriesCounter = new AtomicLong(0L)
  private val rowsValidatedCounter = new AtomicLong(0L)
  private val matchedRowsCounter = new AtomicLong(0L)
  private val mismatchedRowsCounter = new AtomicLong(0L)
  private val missingKeysCounter = new AtomicLong(0L)
  private val binMismatchRowsCounter = new AtomicLong(0L)

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

  private val assignedEndpointsStr: String = (0 until options.writerThreads)
    .map(w => options.endpointFor(partitionId, w))
    .map(ep => s"${ep.host}:${ep.port}")
    .distinct
    .mkString(", ")

  logInfo(
    s"[Executor][Write] Initialized writer for partition=$partitionId, task=$taskId, " +
      s"jobId='${options.jobId}', target='${options.namespace}.${options.setName}', " +
      s"writeMode='${options.writeMode}', validateOnly=${options.validateOnly}, " +
      s"compression='${options.compression}', writerThreads=${options.writerThreads}, " +
      s"batchSize=${options.batchSize}, endpoints=[$assignedEndpointsStr]"
  )

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

      val elapsedMs = math.max(0L, System.currentTimeMillis() - startTimeMs)
      logInfo(
        s"[Executor][Write] Committed partition=$partitionId, task=$taskId, jobId='${options.jobId}' " +
          s"in ${elapsedMs}ms: rowsWritten=${rowsWrittenCounter.get()}, batchesSent=${batchesSentCounter.get()}, " +
          s"overloadRetries=${overloadRetriesCounter.get()}, otherRetries=${otherRetriesCounter.get()}, " +
          s"rowsValidated=${rowsValidatedCounter.get()}, mismatchedRows=${mismatchedRowsCounter.get()}"
      )

      AerospikeFlightCommitMessage(
        partitionId = partitionId,
        taskId = taskId,
        rowsWritten = rowsWrittenCounter.get(),
        batchesSent = batchesSentCounter.get(),
        overloadRetries = overloadRetriesCounter.get(),
        otherRetries = otherRetriesCounter.get(),
        elapsedMs = elapsedMs,
        rowsValidated = rowsValidatedCounter.get(),
        matchedRows = matchedRowsCounter.get(),
        mismatchedRows = mismatchedRowsCounter.get(),
        missingKeys = missingKeysCounter.get(),
        binMismatchRows = binMismatchRowsCounter.get()
      )
    } finally {
      closeResources()
    }
  }

  override def abort(): Unit = {
    logWarning(
      s"[Executor][Write] Aborting writer for partition=$partitionId, task=$taskId, jobId='${options.jobId}' " +
        s"(rowsWrittenSoFar=${rowsWrittenCounter.get()}, batchesSentSoFar=${batchesSentCounter.get()})"
    )
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
      createTaskMetric("as_other_retries", otherRetriesCounter.get()),
      createTaskMetric("as_rows_validated", rowsValidatedCounter.get()),
      createTaskMetric("as_matched_rows", matchedRowsCounter.get()),
      createTaskMetric("as_mismatched_rows", mismatchedRowsCounter.get())
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
      val recordBatch = stagingUnloader.getRecordBatch
      enqueueWithFailureCheck(Some(recordBatch))
      if (!options.validateOnly) {
        rowsWrittenCounter.addAndGet(count.toLong)
      }
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
    val mapper = AerospikeFlightOptions.SharedMapper
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
      FlightCompressionHelper.configureOutboundStream(
        streamListener,
        workerRoot,
        options.compression,
        options.compressionLevel
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
                      val bytes = new Array[Byte](metaBuf.readableBytes().toInt)
                      metaBuf.getBytes(metaBuf.readerIndex(), bytes)
                      val jsonNode = mapper.readTree(bytes)
                      if (jsonNode != null) {
                        val batchOverload = if (jsonNode.has("overload_retries")) {
                          val v = jsonNode.get("overload_retries").asLong(0L)
                          overloadRetriesCounter.addAndGet(v)
                          v
                        } else 0L
                        val batchOther = if (jsonNode.has("other_retries")) {
                          val v = jsonNode.get("other_retries").asLong(0L)
                          otherRetriesCounter.addAndGet(v)
                          v
                        } else 0L
                        if (batchOverload > 0L || batchOther > 0L) {
                          logWarning(
                            s"[Executor][Write] Partition=$partitionId, task=$taskId, worker=$threadIdx " +
                              s"(${endpoint.host}:${endpoint.port}) batch experienced retries on proxy: " +
                              s"overloadRetries=$batchOverload, otherRetries=$batchOther"
                          )
                        }
                        if (jsonNode.has("rows_validated")) {
                          rowsValidatedCounter.addAndGet(jsonNode.get("rows_validated").asLong(0L))
                        }
                        if (jsonNode.has("matched_rows")) {
                          matchedRowsCounter.addAndGet(jsonNode.get("matched_rows").asLong(0L))
                        }
                        if (jsonNode.has("mismatched_rows")) {
                          mismatchedRowsCounter.addAndGet(jsonNode.get("mismatched_rows").asLong(0L))
                        }
                        if (jsonNode.has("missing_keys")) {
                          missingKeysCounter.addAndGet(jsonNode.get("missing_keys").asLong(0L))
                        }
                        if (jsonNode.has("bin_mismatch_rows")) {
                          binMismatchRowsCounter.addAndGet(jsonNode.get("bin_mismatch_rows").asLong(0L))
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
        logError(
          s"[Executor][Write] Sender worker=$threadIdx failed in partition=$partitionId, task=$taskId, jobId='${options.jobId}': ${t.getMessage}",
          t
        )
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
