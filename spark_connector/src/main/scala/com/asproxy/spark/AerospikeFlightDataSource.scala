package com.asproxy.spark

import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._

/**
 * Spark 3.5 DataSource V2 entry point for writing DataFrames to Aerospike via the Rust Arrow Flight Proxy.
 *
 * Usage:
 * {{{
 *   df.write
 *     .format("aerospike-flight")
 *     .option("flight.hosts", "proxy1:8815,proxy2:8815")
 *     .option("flight.username", "spark_writer")
 *     .option("flight.password", "secret")
 *     .option("aerospike.namespace", "test")
 *     .option("aerospike.set", "users")
 *     .option("aerospike.keyColumn", "user_id")
 *     .option("aerospike.writeMode", "update")
 *     .mode("append")
 *     .save()
 * }}}
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
    val schema: StructType,
    val options: AerospikeFlightOptions
) extends Table
    with SupportsWrite {

  override def name(): String = s"aerospike-flight:${options.namespace}.${options.setName}"

  override def capabilities(): util.Set[TableCapability] = {
    Set(
      TableCapability.BATCH_WRITE,
      TableCapability.STREAMING_WRITE,
      TableCapability.ACCEPT_ANY_SCHEMA
    ).asJava
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    val mergedProps = options
    new AerospikeFlightWriteBuilder(info.schema(), mergedProps)
  }
}
