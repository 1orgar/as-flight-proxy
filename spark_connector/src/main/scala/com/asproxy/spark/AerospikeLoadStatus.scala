package com.asproxy.spark

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.arrow.flight.{
  Action,
  CallOption,
  CallOptions,
  FlightCallHeaders,
  FlightClient,
  HeaderCallOption,
  Location
}
import org.apache.arrow.flight.grpc.CredentialCallOption
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomSumMetric}

import java.io.FileInputStream
import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

/**
 * Aggregated load summary produced on the Spark Driver when a `df.write.format("aerospike-flight")`
 * batch or streaming micro-batch finishes.
 */
case class LoadJobSummary(
    jobId: String,
    namespace: String,
    setName: String,
    status: String,
    rowsWritten: Long,
    batchesSent: Long,
    overloadRetries: Long,
    otherRetries: Long,
    partitionsCount: Int,
    elapsedMs: Long,
    raftClusterStatusJson: Option[String] = None,
    validateOnly: Boolean = false,
    rowsValidated: Long = 0L,
    matchedRows: Long = 0L,
    mismatchedRows: Long = 0L,
    missingKeys: Long = 0L,
    binMismatchRows: Long = 0L,
    dqPassed: Option[Boolean] = None
) extends Serializable {

  def toJson: String = {
    val mapper = AerospikeFlightOptions.SharedMapper
    val map = new util.LinkedHashMap[String, Any]()
    map.put("job_id", jobId)
    map.put("namespace", namespace)
    map.put("set_name", setName)
    map.put("status", status)
    map.put("is_dq_validation", Boolean.box(validateOnly))
    map.put("rows_written", Long.box(rowsWritten))
    map.put("batches_sent", Long.box(batchesSent))
    map.put("overload_retries", Long.box(overloadRetries))
    map.put("other_retries", Long.box(otherRetries))
    map.put("rows_validated", Long.box(rowsValidated))
    map.put("matched_rows", Long.box(matchedRows))
    map.put("mismatched_rows", Long.box(mismatchedRows))
    map.put("missing_keys", Long.box(missingKeys))
    map.put("bin_mismatch_rows", Long.box(binMismatchRows))
    dqPassed.foreach(passed => map.put("dq_passed", Boolean.box(passed)))
    map.put("partitions_count", Int.box(partitionsCount))
    map.put("elapsed_ms", Long.box(elapsedMs))
    raftClusterStatusJson.foreach(raw => map.put("raft_cluster_status", mapper.readTree(raw)))
    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(map)
  }
}

/**
 * Driver-side registry and Arrow Flight RPC helper for querying load progress and Raft cluster status.
 */
object AerospikeLoadStatus extends Logging {
  private val summariesByJobId = new ConcurrentHashMap[String, LoadJobSummary]()
  private val latestSummaryRef = new AtomicReference[Option[LoadJobSummary]](None)

  /**
   * Record a completed or failed Spark write job summary on the driver.
   */
  def recordSummary(summary: LoadJobSummary): Unit = {
    summariesByJobId.put(summary.jobId, summary)
    latestSummaryRef.set(Some(summary))
  }

  /**
   * Returns the summary of the most recently completed `df.write.format("aerospike-flight")` job.
   */
  def lastSummary: Option[LoadJobSummary] = latestSummaryRef.get()

  /**
   * PySpark / Py4J-friendly helper returning the latest job summary as a JSON string (or `"{}"`).
   */
  def getLastSummaryJson(): String =
    lastSummary.map(_.toJson).getOrElse("{}")

  /**
   * Alias for `getLastSummaryJson()` matching Scala/PySpark documentation examples.
   */
  def latestSummaryJson(): String = getLastSummaryJson()

  /**
   * Returns the summary for a specific `connector.jobId`.
   */
  def getSummary(jobId: String): Option[LoadJobSummary] =
    Option(summariesByJobId.get(jobId))

  /**
   * PySpark / Py4J-friendly helper returning the summary for `jobId` as a JSON string (or `"{}"`).
   */
  def getSummaryJson(jobId: String): String =
    getSummary(jobId).map(_.toJson).getOrElse("{}")

  /**
   * Returns all summaries recorded in the current Spark Driver session.
   */
  def allSummaries: Seq[LoadJobSummary] = {
    import scala.collection.JavaConverters._
    summariesByJobId.values().asScala.toSeq
  }

  /**
   * Clear recorded summaries (useful in tests).
   */
  def clear(): Unit = {
    summariesByJobId.clear()
    latestSummaryRef.set(None)
  }

  /**
   * Query any `proxy_server` node via Arrow Flight `DoAction("get_job_status", jobId)`
   * to retrieve the real-time Raft-replicated status across all proxy nodes.
   */
  def fetchRemoteJobStatus(options: AerospikeFlightOptions, jobId: String = ""): String = {
    val targetJobId = if (jobId.trim.nonEmpty) jobId.trim else options.jobId
    executeAction(options, "get_job_status", targetJobId.getBytes(StandardCharsets.UTF_8))
  }

  /**
   * PySpark / Py4J-friendly method to query real-time Raft-replicated job status from `proxy_server`.
   */
  def fetchRemoteJobStatusJson(
      hosts: String,
      jobId: String,
      username: String = "",
      password: String = "",
      tlsEnabled: Boolean = false
  ): String = {
    val opts = AerospikeFlightOptions.forStatusQuery(
      Map(
        "flight.hosts" -> hosts,
        "connector.jobId" -> jobId,
        "flight.username" -> username,
        "flight.password" -> password,
        "flight.tls.enabled" -> tlsEnabled.toString
      )
    )
    fetchRemoteJobStatus(opts, jobId)
  }

  /**
   * Query any `proxy_server` node via Arrow Flight `DoAction("get_cluster_status", "")`
   * to retrieve the Raft cluster snapshot (leader, term, peer nodes, and all jobs).
   */
  def fetchClusterStatus(options: AerospikeFlightOptions): String = {
    executeAction(options, "get_cluster_status", Array.emptyByteArray)
  }

  /**
   * PySpark / Py4J-friendly method to query Raft cluster status from `proxy_server`.
   */
  def fetchClusterStatusJson(
      hosts: String,
      username: String = "",
      password: String = "",
      tlsEnabled: Boolean = false
  ): String = {
    val opts = AerospikeFlightOptions.forStatusQuery(
      Map(
        "flight.hosts" -> hosts,
        "flight.username" -> username,
        "flight.password" -> password,
        "flight.tls.enabled" -> tlsEnabled.toString
      )
    )
    fetchClusterStatus(opts)
  }

  /**
   * Initialize or reset Raft-replicated metadata for `options.jobId` at the start of a Spark job,
   * overwriting any previous job metadata with the same `jobId`.
   */
  def initJob(options: AerospikeFlightOptions): Option[String] = {
    if (options.jobId != null && options.jobId.trim.nonEmpty) {
      summariesByJobId.remove(options.jobId)
    }
    notifyJobState(options, "init_job")
  }

  /**
   * Notify the `proxy_server` Raft cluster that a Spark job has initialized, completed, or failed.
   */
  def notifyJobState(
      options: AerospikeFlightOptions,
      actionType: String,
      errorMsg: Option[String] = None
  ): Option[String] = {
    try {
      val payload = new util.LinkedHashMap[String, Any]()
      payload.put("job_id", options.jobId)
      payload.put("namespace", options.namespace)
      payload.put("set_name", options.setName)
      payload.put("is_dq_validation", Boolean.box(options.validateOnly))
      errorMsg.foreach(e => payload.put("error", e))
      val bytes = AerospikeFlightOptions.SharedMapper.writeValueAsBytes(payload)
      Some(executeAction(options, actionType, bytes))
    } catch {
      case t: Throwable =>
        logDebug(
          s"[Driver][RaftStatus] Could not execute Flight action '$actionType' for jobId='${options.jobId}' (Raft may be disabled): ${t.getMessage}"
        )
        None
    }
  }

  private def executeAction(
      options: AerospikeFlightOptions,
      actionType: String,
      body: Array[Byte]
  ): String = {
    val allocator = new RootAllocator(Long.MaxValue)
    var lastErr: Throwable = null
    try {
      var idx = 0
      while (idx < options.hosts.size) {
        val endpoint = options.hosts(idx)
        var client: FlightClient = null
        try {
          client = createFlightClient(allocator, endpoint, options)
          val callOpts = buildCallOptions(client, options)
          val results = client.doAction(new Action(actionType, body), callOpts: _*)
          return if (results.hasNext) {
            val res = results.next()
            new String(res.getBody, StandardCharsets.UTF_8)
          } else {
            "{}"
          }
        } catch {
          case t: Throwable =>
            lastErr = t
            logWarning(
              s"[Driver][RaftStatus] Flight action '$actionType' failed on ${endpoint.host}:${endpoint.port} " +
                s"(attempt ${idx + 1}/${options.hosts.size}): ${t.getMessage}"
            )
            idx += 1
        } finally {
          if (client != null) {
            try { client.close() } catch { case _: Throwable => }
          }
        }
      }
      throw new RuntimeException(
        s"Failed to execute Flight action '$actionType' across endpoints ${options.hosts.mkString(", ")}",
        lastErr
      )
    } finally {
      try { allocator.close() } catch { case _: Throwable => }
    }
  }

  private[spark] def createFlightClient(
      allocator: BufferAllocator,
      endpoint: HostEndpoint,
      options: AerospikeFlightOptions
  ): FlightClient = {
    val location = if (options.tlsEnabled) {
      Location.forGrpcTls(endpoint.host, endpoint.port)
    } else {
      Location.forGrpcInsecure(endpoint.host, endpoint.port)
    }

    val builder = FlightClient.builder(allocator, location)
    if (options.tlsEnabled) {
      builder.useTls()
      if (!options.tlsVerifyServer) {
        builder.verifyServer(false)
      }
      options.tlsCaCertPath.foreach { caPath =>
        val in = new FileInputStream(caPath)
        try {
          builder.trustedCertificates(in)
        } finally {
          in.close()
        }
      }
      for {
        certPath <- options.tlsClientCertPath
        keyPath  <- options.tlsClientKeyPath
      } {
        val certIn = new FileInputStream(certPath)
        val keyIn = new FileInputStream(keyPath)
        try {
          builder.clientCertificate(certIn, keyIn)
        } finally {
          certIn.close()
          keyIn.close()
        }
      }
    }

    builder.build()
  }

  private[spark] def buildCallOptions(
      client: FlightClient,
      options: AerospikeFlightOptions
  ): Seq[CallOption] = {
    val opts = scala.collection.mutable.ArrayBuffer.empty[CallOption]
    if (options.username.nonEmpty) {
      val credOpt: CredentialCallOption = client
        .authenticateBasicToken(options.username, options.password)
        .orElseThrow(new java.util.function.Supplier[RuntimeException] {
          override def get(): RuntimeException =
            new RuntimeException("Arrow Flight authentication did not return a Bearer token")
        })
      opts += credOpt
    }
    val headers = new FlightCallHeaders()
    headers.insert("x-as-compression", options.compression)
    headers.insert("x-as-compression-level", options.compressionLevel.toString)
    opts += new HeaderCallOption(headers)
    FlightCompressionHelper.grpcCompressionCallOption(options.compression).foreach(opts += _)
    if (options.timeoutMs > 0) {
      opts += CallOptions.timeout(options.timeoutMs, TimeUnit.MILLISECONDS)
    }
    opts.toSeq
  }
}

/**
 * Spark SQL UI custom metrics reported by executors during `df.write.format("aerospike-flight")`.
 */
class AerospikeRowsWrittenMetric extends CustomSumMetric {
  override def name(): String = "as_rows_written"
  override def description(): String = "Aerospike rows written via Arrow Flight"
}

class AerospikeBatchesSentMetric extends CustomSumMetric {
  override def name(): String = "as_batches_sent"
  override def description(): String = "Arrow RecordBatches sent to proxy_server"
}

class AerospikeOverloadRetriesMetric extends CustomSumMetric {
  override def name(): String = "as_overload_retries"
  override def description(): String = "Aerospike overload retries (DEVICE_OVERLOAD / KEY_BUSY)"
}

class AerospikeOtherRetriesMetric extends CustomSumMetric {
  override def name(): String = "as_other_retries"
  override def description(): String = "Aerospike non-overload retries"
}

class AerospikeRowsValidatedMetric extends CustomSumMetric {
  override def name(): String = "as_rows_validated"
  override def description(): String = "Aerospike DQ rows validated"
}

class AerospikeMatchedRowsMetric extends CustomSumMetric {
  override def name(): String = "as_matched_rows"
  override def description(): String = "Aerospike DQ matched rows"
}

class AerospikeMismatchedRowsMetric extends CustomSumMetric {
  override def name(): String = "as_mismatched_rows"
  override def description(): String = "Aerospike DQ mismatched rows"
}

class AerospikeRowsReadMetric extends CustomSumMetric {
  override def name(): String = "as_rows_read"
  override def description(): String = "Aerospike rows read via Arrow Flight"
}

class AerospikeBatchesReadMetric extends CustomSumMetric {
  override def name(): String = "as_batches_read"
  override def description(): String = "Arrow RecordBatches received from proxy_server"
}

object AerospikeCustomMetrics {
  def supportedMetrics: Array[CustomMetric] = Array(
    new AerospikeRowsWrittenMetric(),
    new AerospikeBatchesSentMetric(),
    new AerospikeOverloadRetriesMetric(),
    new AerospikeOtherRetriesMetric(),
    new AerospikeRowsValidatedMetric(),
    new AerospikeMatchedRowsMetric(),
    new AerospikeMismatchedRowsMetric()
  )

  def supportedReadMetrics: Array[CustomMetric] = Array(
    new AerospikeRowsReadMetric(),
    new AerospikeBatchesReadMetric()
  )
}
