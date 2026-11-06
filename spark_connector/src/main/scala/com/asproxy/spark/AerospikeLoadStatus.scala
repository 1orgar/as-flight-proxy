package com.asproxy.spark

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.arrow.flight.{Action, CallOption, CallOptions, FlightClient, Location}
import org.apache.arrow.flight.grpc.CredentialCallOption
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
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
    raftClusterStatusJson: Option[String] = None
) extends Serializable {

  def toJson: String = {
    val mapper = new ObjectMapper()
    val map = new util.LinkedHashMap[String, Any]()
    map.put("job_id", jobId)
    map.put("namespace", namespace)
    map.put("set_name", setName)
    map.put("status", status)
    map.put("rows_written", Long.box(rowsWritten))
    map.put("batches_sent", Long.box(batchesSent))
    map.put("overload_retries", Long.box(overloadRetries))
    map.put("other_retries", Long.box(otherRetries))
    map.put("partitions_count", Int.box(partitionsCount))
    map.put("elapsed_ms", Long.box(elapsedMs))
    raftClusterStatusJson.foreach(raw => map.put("raft_cluster_status", mapper.readTree(raw)))
    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(map)
  }
}

/**
 * Driver-side registry and Arrow Flight RPC helper for querying load progress and Raft cluster status.
 */
object AerospikeLoadStatus {
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
   * Returns the summary for a specific `connector.jobId`.
   */
  def getSummary(jobId: String): Option[LoadJobSummary] =
    Option(summariesByJobId.get(jobId))

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
   * Query any `proxy_server` node via Arrow Flight `DoAction("get_cluster_status", "")`
   * to retrieve the Raft cluster snapshot (leader, term, peer nodes, and all jobs).
   */
  def fetchClusterStatus(options: AerospikeFlightOptions): String = {
    executeAction(options, "get_cluster_status", Array.emptyByteArray)
  }

  /**
   * Notify the `proxy_server` Raft cluster that a Spark job has completed or failed.
   */
  def notifyJobState(
      options: AerospikeFlightOptions,
      actionType: String,
      errorMsg: Option[String] = None
  ): Option[String] = {
    try {
      val mapper = new ObjectMapper()
      val payload = new util.LinkedHashMap[String, Any]()
      payload.put("job_id", options.jobId)
      payload.put("namespace", options.namespace)
      payload.put("set_name", options.setName)
      errorMsg.foreach(e => payload.put("error", e))
      val bytes = mapper.writeValueAsBytes(payload)
      Some(executeAction(options, actionType, bytes))
    } catch {
      case _: Throwable => None
    }
  }

  private def executeAction(
      options: AerospikeFlightOptions,
      actionType: String,
      body: Array[Byte]
  ): String = {
    val allocator = new RootAllocator(Long.MaxValue)
    var client: FlightClient = null
    try {
      val endpoint = options.hosts.head
      client = createFlightClient(allocator, endpoint, options)
      val callOpts = buildCallOptions(client, options)
      val results = client.doAction(new Action(actionType, body), callOpts: _*)
      val iter = results.iterator()
      if (iter.hasNext) {
        val res = iter.next()
        new String(res.getBody, StandardCharsets.UTF_8)
      } else {
        "{}"
      }
    } finally {
      if (client != null) {
        try { client.close() } catch { case _: Throwable => }
      }
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

object AerospikeCustomMetrics {
  def supportedMetrics: Array[CustomMetric] = Array(
    new AerospikeRowsWrittenMetric(),
    new AerospikeBatchesSentMetric(),
    new AerospikeOverloadRetriesMetric(),
    new AerospikeOtherRetriesMetric()
  )
}
