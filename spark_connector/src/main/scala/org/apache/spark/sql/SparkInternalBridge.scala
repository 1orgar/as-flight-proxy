package org.apache.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

/**
 * Package-private Spark SQL bridge to construct a `DataFrame` directly from an `RDD[InternalRow]`
 * without expensive `InternalRow -> external Row -> InternalRow` Catalyst encoder round-trips.
 */
object SparkInternalBridge {
  def createDataFrame(
      spark: SparkSession,
      catalystRows: RDD[InternalRow],
      schema: StructType
  ): DataFrame = {
    spark.internalCreateDataFrame(catalystRows, schema, isStreaming = false)
  }
}
