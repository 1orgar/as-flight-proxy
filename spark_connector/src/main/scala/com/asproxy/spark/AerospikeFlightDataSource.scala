package com.asproxy.spark

import org.apache.spark.sql.connector.catalog.{
  SupportsWrite,
  Table,
  TableCapability,
  TableProvider
}
import org.apache.spark.sql.connector.expressions.Transform
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

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = new StructType()

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]
  ): Table = {
    val options = AerospikeFlightOptions.fromMap(properties.asScala.toMap)
    new AerospikeFlightTable(schema, options)
  }
}

class AerospikeFlightTable(
    val tableSchema: StructType,
    val options: AerospikeFlightOptions
) extends Table
    with SupportsWrite {

  override def name(): String = s"aerospike-flight:${options.namespace}.${options.setName}"

  override def schema(): StructType = tableSchema

  override def capabilities(): util.Set[TableCapability] = {
    Set(
      TableCapability.BATCH_WRITE,
      TableCapability.STREAMING_WRITE,
      TableCapability.ACCEPT_ANY_SCHEMA
    ).asJava
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    options.validateWriteTarget()
    new AerospikeFlightWriteBuilder(info.schema(), options)
  }
}
