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
    val parsedOpts = AerospikeFlightOptions.fromSparkOptions(options)
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
    val options = AerospikeFlightOptions.fromMap(properties.asScala.toMap)
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
          AerospikeSchemaValidator.validate(StructType(keyField +: binsOnlyFields), options)
        }
        outSchema
      case _ =>
        if (schema != null && schema.fields.nonEmpty) {
          AerospikeSchemaValidator.validate(schema, options)
        }
        schema
    }
    new AerospikeFlightTable(effectiveSchema, options)
  }
}

class AerospikeFlightTable(
    val tableSchema: StructType,
    val options: AerospikeFlightOptions
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

  override def newScanBuilder(scanOptions: CaseInsensitiveStringMap): ScanBuilder = {
    val mergedMap = options match {
      case _ if scanOptions.isEmpty => options
      case _ =>
        AerospikeFlightOptions.fromSparkOptions(scanOptions)
    }
    new AerospikeFlightScanBuilder(tableSchema, mergedMap)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    AerospikeSchemaValidator.validate(info.schema(), options)
    new AerospikeFlightWriteBuilder(info.schema(), options)
  }
}
