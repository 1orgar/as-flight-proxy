package com.asproxy.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{
  SupportsRead,
  SupportsWrite,
  Table,
  TableCapability,
  TableProvider
}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._

/**
 * Spark 3.5 DataSource V2 entry point for Aerospike via the Rust Arrow Flight MPP Proxy.
 */
class AerospikeFlightDataSource extends TableProvider with DataSourceRegister {

  override def shortName(): String = "aerospike-flight"

  override def supportsExternalMetadata(): Boolean = true

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val parsedOpts = AerospikeFlightOptions.fromSparkReadOptions(options)
    parsedOpts.lookupSourceView match {
      case Some(viewName) =>
        val sourceDf = SparkSession.active.table(viewName)
        val rawBinsDdl = Option(options.get("aerospike.lookup.binsSchema"))
          .orElse(Option(options.get("aerospike.lookup.bins_schema")))
          .map(_.trim)
          .filter(_.nonEmpty)
        val binsSchema = rawBinsDdl match {
          case Some(ddl) => AerospikeFlightLookup.parseBinsSchema(ddl)
          case None      => AerospikeFlightSchemaInferrer.inferSchema(parsedOpts)
        }
        AerospikeFlightLookup.buildOutputSchema(
          sourceDf.schema,
          binsSchema,
          parsedOpts.keyColumn,
          parsedOpts.lookupJoinType
        )
      case None =>
        AerospikeFlightSchemaInferrer.inferSchema(parsedOpts)
    }
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]
  ): Table = {
    val rawProps = properties.asScala.toMap
    val options = AerospikeFlightOptions.fromTableOrReadMap(rawProps)
    val effectiveSchema = options.lookupSourceView match {
      case Some(viewName) if schema != null && schema.fields.nonEmpty =>
        val sourceDf = SparkSession.active.table(viewName)
        val outSchema = AerospikeFlightLookup.buildOutputSchema(
          sourceDf.schema,
          schema,
          options.keyColumn,
          options.lookupJoinType
        )
        val keyField = sourceDf.schema.fields
          .find(_.name == options.keyColumn)
          .getOrElse(
            throw new IllegalArgumentException(
              s"Primary key column '${options.keyColumn}' not found in lookup source view '$viewName'"
            )
          )
        val binsOnlyFields = outSchema.fields.filterNot(f => sourceDf.schema.fields.exists(_.name == f.name))
        if (binsOnlyFields.nonEmpty) {
          AerospikeSchemaValidator.validate(
            StructType(keyField +: binsOnlyFields),
            options,
            requireWriteJobId = false
          )
        }
        outSchema
      case _ =>
        if (schema != null && schema.fields.nonEmpty) {
          AerospikeSchemaValidator.validate(schema, options, requireWriteJobId = false)
        }
        schema
    }
    new AerospikeFlightTable(effectiveSchema, options, rawProps)
  }
}

class AerospikeFlightTable(
    val tableSchema: StructType,
    val options: AerospikeFlightOptions,
    val rawProperties: Map[String, String] = Map.empty
) extends Table
    with SupportsRead
    with SupportsWrite {

  override def name(): String = s"aerospike-flight:${options.namespace}.${options.setName}"

  override def schema(): StructType = tableSchema

  override def capabilities(): util.Set[TableCapability] = {
    Set(
      TableCapability.BATCH_READ,
      TableCapability.BATCH_WRITE,
      TableCapability.STREAMING_WRITE,
      TableCapability.TRUNCATE,
      TableCapability.ACCEPT_ANY_SCHEMA
    ).asJava
  }

  private def mergeWithBaseProperties(overrides: Map[String, String]): Map[String, String] = {
    val baseProps: Map[String, String] = if (rawProperties.nonEmpty) {
      rawProperties
    } else {
      val fallback = scala.collection.mutable.LinkedHashMap[String, String](
        "flight.hosts" -> options.hosts.map(_.toString).mkString(","),
        "aerospike.namespace" -> options.namespace,
        "aerospike.set" -> options.setName,
        "aerospike.keyColumn" -> options.keyColumn,
        "aerospike.writeMode" -> options.writeMode
      )
      if (options.hasExplicitJobId) {
        fallback.put("connector.jobId", options.jobId)
      }
      fallback.toMap
    }
    val baseLowerKeys = baseProps.map { case (k, v) => k.toLowerCase -> (k -> v) }
    val mergedLower = overrides.foldLeft(baseLowerKeys) { case (acc, (k, v)) =>
      acc.updated(k.toLowerCase, k -> v)
    }
    mergedLower.values.toMap
  }

  override def newScanBuilder(scanOptions: CaseInsensitiveStringMap): ScanBuilder = {
    val effectiveOptions = if (scanOptions == null || scanOptions.isEmpty) {
      options
    } else {
      val scanMap = scanOptions.asCaseSensitiveMap().asScala.toMap
      AerospikeFlightOptions.fromTableOrReadMap(mergeWithBaseProperties(scanMap))
    }
    new AerospikeFlightScanBuilder(tableSchema, effectiveOptions)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    val writeOpts = if (info.options() == null || info.options().isEmpty) {
      options
    } else {
      val writeMap = info.options().asCaseSensitiveMap().asScala.toMap
      AerospikeFlightOptions.fromMap(mergeWithBaseProperties(writeMap), requireWriteTarget = true)
    }
    writeOpts.validateWriteTarget()
    AerospikeSchemaValidator.validate(info.schema(), writeOpts)
    new AerospikeFlightWriteBuilder(info.schema(), writeOpts)
  }
}
