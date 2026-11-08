package com.asproxy.spark

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import java.util.UUID
import scala.collection.JavaConverters._

/**
 * Configuration options for the Spark -> Arrow Flight -> Aerospike Proxy connector.
 */
case class HostEndpoint(host: String, port: Int) extends Serializable {
  override def toString: String = s"$host:$port"
}

case class AerospikeFlightOptions(
    jobId: String,
    hosts: Seq[HostEndpoint],
    username: String,
    password: String,
    tlsEnabled: Boolean,
    tlsCaCertPath: Option[String],
    tlsClientCertPath: Option[String],
    tlsClientKeyPath: Option[String],
    tlsVerifyServer: Boolean,
    timeoutMs: Long,
    namespace: String,
    setName: String,
    keyColumn: String,
    writeMode: String,
    ttlSeconds: Option[Long],
    sendKey: Boolean,
    ignoreNulls: Boolean,
    useBatchWrite: Option[Boolean],
    validateOnly: Boolean = false,
    batchSize: Int = 8192,
    writerThreads: Int = 4,
    queueCapacity: Int = 8
) extends Serializable {

  require(jobId.trim.nonEmpty, "'connector.jobId' cannot be empty")
  require(hosts.nonEmpty, "'flight.hosts' must contain at least one host:port endpoint")
  require(batchSize > 0, s"'connector.batchSize' must be > 0 (got $batchSize)")
  require(writerThreads > 0, s"'connector.writerThreads' must be > 0 (got $writerThreads)")
  require(queueCapacity > 0, s"'connector.queueCapacity' must be > 0 (got $queueCapacity)")

  def validateWriteTarget(): Unit = {
    require(namespace.trim.nonEmpty, "'aerospike.namespace' cannot be empty")
    require(keyColumn.trim.nonEmpty, "'aerospike.keyColumn' cannot be empty")
  }

  /**
   * Select a proxy endpoint for a given Spark partition and worker thread index
   * to distribute load evenly across all proxy instances.
   */
  def endpointFor(partitionId: Int, threadIndex: Int): HostEndpoint = {
    val idx = math.abs(partitionId + threadIndex) % hosts.size
    hosts(idx)
  }

  /**
   * Serialize per-stream write parameters into the JSON payload expected in `FlightDescriptor.cmd`
   * by the Rust `proxy_server`.
   */
  def toFlightDescriptorJsonBytes: Array[Byte] = {
    val mapper = new ObjectMapper()
    val payload = new util.LinkedHashMap[String, Any]()
    payload.put("job_id", jobId)
    payload.put("namespace", namespace)
    payload.put("set_name", setName)
    payload.put("key_column", keyColumn)
    payload.put("write_mode", writeMode.toLowerCase)
    ttlSeconds.foreach(ttl => payload.put("ttl_seconds", Long.box(ttl)))
    payload.put("send_key", Boolean.box(sendKey))
    payload.put("ignore_nulls", Boolean.box(ignoreNulls))
    useBatchWrite.foreach(ub => payload.put("use_batch_write", Boolean.box(ub)))
    payload.put("validate_only", Boolean.box(validateOnly))
    mapper.writeValueAsBytes(payload)
  }
}

object AerospikeFlightOptions {
  private val AllowedWriteModes = Set(
    "update",
    "replace",
    "update_only",
    "replace_only",
    "create_only",
    "validate",
    "dq",
    "verify"
  )

  def fromSparkOptions(options: CaseInsensitiveStringMap): AerospikeFlightOptions = {
    fromMap(options.asCaseSensitiveMap().asScala.toMap, requireWriteTarget = true)
  }

  def forStatusQuery(raw: Map[String, String]): AerospikeFlightOptions = {
    fromMap(raw, requireWriteTarget = false)
  }

  def fromMap(raw: Map[String, String], requireWriteTarget: Boolean = true): AerospikeFlightOptions = {
    val ci = raw.map { case (k, v) => k.toLowerCase -> v }

    def get(key: String): Option[String] = ci.get(key.toLowerCase).map(_.trim).filter(_.nonEmpty)
    def getOrElse(key: String, default: String): String = get(key).getOrElse(default)
    def getRequired(key: String): String =
      get(key).getOrElse(throw new IllegalArgumentException(s"Missing required option '$key'"))

    val jobId = get("connector.jobId")
      .orElse(get("aerospike.job_id"))
      .orElse(get("aerospike.jobId"))
      .orElse(get("job.id"))
      .getOrElse(s"as-load-${UUID.randomUUID().toString}")

    val rawHosts = get("flight.hosts")
      .orElse(get("aerospike.flight.endpoint"))
      .orElse(get("aerospike.flight.hosts"))
      .getOrElse("127.0.0.1:8815")
    val hosts = parseHosts(rawHosts)

    val username = get("flight.username")
      .orElse(get("aerospike.flight.username"))
      .getOrElse("")
    val password = get("flight.password")
      .orElse(get("aerospike.flight.password"))
      .getOrElse("")

    val tlsEnabled = get("flight.tls.enabled")
      .orElse(get("aerospike.flight.tls.enabled"))
      .getOrElse("false")
      .toBoolean
    val tlsCaCertPath = get("flight.tls.caCertPath")
      .orElse(get("aerospike.flight.tls.ca_cert_path"))
      .orElse(get("aerospike.flight.tls.caCertPath"))
    val tlsClientCertPath = get("flight.tls.clientCertPath")
      .orElse(get("aerospike.flight.tls.client_cert_path"))
      .orElse(get("aerospike.flight.tls.clientCertPath"))
    val tlsClientKeyPath = get("flight.tls.clientKeyPath")
      .orElse(get("aerospike.flight.tls.client_key_path"))
      .orElse(get("aerospike.flight.tls.clientKeyPath"))
    val tlsVerifyServer = get("flight.tls.verifyServer")
      .orElse(get("aerospike.flight.tls.verify_server"))
      .getOrElse("true")
      .toBoolean
    val timeoutMs = get("flight.timeoutMs")
      .orElse(get("aerospike.flight.timeout_ms"))
      .getOrElse("60000")
      .toLong

    val namespace = if (requireWriteTarget) getRequired("aerospike.namespace") else getOrElse("aerospike.namespace", "")
    val setName = getOrElse("aerospike.set", "")
    val keyColumn = if (requireWriteTarget) {
      get("aerospike.keyColumn")
        .orElse(get("aerospike.key_column"))
        .getOrElse(throw new IllegalArgumentException("Missing required option 'aerospike.keyColumn' (or 'aerospike.key_column')"))
    } else {
      get("aerospike.keyColumn").orElse(get("aerospike.key_column")).getOrElse("")
    }

    val writeMode = get("aerospike.writeMode")
      .orElse(get("aerospike.write_mode"))
      .getOrElse("update")
      .toLowerCase
    if (!AllowedWriteModes.contains(writeMode)) {
      throw new IllegalArgumentException(
        s"Unsupported 'aerospike.writeMode' = '$writeMode'. Allowed: ${AllowedWriteModes.mkString(", ")}"
      )
    }

    val ttlSeconds = get("aerospike.ttl").orElse(get("aerospike.ttl_seconds")).map(_.toLong)
    val sendKey = get("aerospike.sendKey").orElse(get("aerospike.send_key")).getOrElse("true").toBoolean
    // Default is false: null values in update mode send Value::Nil to delete the bin
    val ignoreNulls = get("aerospike.ignoreNulls").orElse(get("aerospike.ignore_nulls")).getOrElse("false").toBoolean
    val useBatchWrite = get("aerospike.useBatchWrite").orElse(get("aerospike.use_batch_write")).map(_.toBoolean)
    val validateOnly = get("aerospike.dq")
      .orElse(get("aerospike.validateOnly"))
      .orElse(get("aerospike.validate_only"))
      .orElse(get("connector.dq"))
      .map(_.toBoolean)
      .getOrElse(writeMode == "validate" || writeMode == "dq" || writeMode == "verify")

    val batchSize = get("connector.batchSize")
      .orElse(get("aerospike.batch_size"))
      .orElse(get("aerospike.batchSize"))
      .getOrElse("8192")
      .toInt
    val writerThreads = get("connector.writerThreads")
      .orElse(get("aerospike.writer_threads"))
      .orElse(get("aerospike.writerThreads"))
      .getOrElse("4")
      .toInt
    val queueCapacity = get("connector.queueCapacity")
      .orElse(get("aerospike.queue_capacity"))
      .orElse(get("aerospike.queueCapacity"))
      .map(_.toInt)
      .getOrElse(math.max(writerThreads * 2, 4))

    AerospikeFlightOptions(
      jobId = jobId,
      hosts = hosts,
      username = username,
      password = password,
      tlsEnabled = tlsEnabled,
      tlsCaCertPath = tlsCaCertPath,
      tlsClientCertPath = tlsClientCertPath,
      tlsClientKeyPath = tlsClientKeyPath,
      tlsVerifyServer = tlsVerifyServer,
      timeoutMs = timeoutMs,
      namespace = namespace,
      setName = setName,
      keyColumn = keyColumn,
      writeMode = writeMode,
      ttlSeconds = ttlSeconds,
      sendKey = sendKey,
      ignoreNulls = ignoreNulls,
      useBatchWrite = useBatchWrite,
      validateOnly = validateOnly,
      batchSize = batchSize,
      writerThreads = writerThreads,
      queueCapacity = queueCapacity
    )
  }

  private def parseHosts(hostsStr: String): Seq[HostEndpoint] = {
    hostsStr
      .split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
      .map { entry =>
        val lastColon = entry.lastIndexOf(':')
        if (lastColon <= 0 || lastColon == entry.length - 1) {
          throw new IllegalArgumentException(
            s"Invalid host endpoint '$entry' in 'flight.hosts'. Expected format 'host:port'"
          )
        }
        val host = entry.substring(0, lastColon).trim
        val port = entry.substring(lastColon + 1).trim.toInt
        HostEndpoint(host, port)
      }
      .toSeq
  }
}
