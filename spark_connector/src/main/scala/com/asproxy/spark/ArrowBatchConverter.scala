package com.asproxy.spark

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.types._

/**
 * Populates an Apache Arrow `VectorSchemaRoot` from Spark Catalyst `InternalRow` records.
 *
 * Pre-compiles per-column writer closures once per `VectorSchemaRoot` so the hot `appendRow` loop
 * avoids per-cell `DataType` pattern matching and `FieldVector` casts.
 */
class ArrowBatchConverter(
    sparkSchema: StructType,
    keyColumn: String,
    root: VectorSchemaRoot,
    ignoreNulls: Boolean = false,
    validateOnly: Boolean = false
) {

  private val fields: Array[StructField] = sparkSchema.fields
  private val keyColumnIndex: Int = {
    val idx = sparkSchema.fieldIndex(keyColumn)
    if (idx < 0) {
      throw new IllegalArgumentException(s"Key column '$keyColumn' not found in DataFrame schema")
    }
    idx
  }

  // Indices of non-key columns in the input Spark InternalRow that can carry non-null values
  private val nonKeySparkOrdinals: Array[Int] = fields.indices
    .filter(i => i != keyColumnIndex && !(ignoreNulls && !validateOnly && fields(i).dataType == NullType))
    .toArray

  // Pre-compiled per-column writer functions `(row: InternalRow, targetRowIdx: Int) => Unit`
  private val columnWriters: Array[(InternalRow, Int) => Unit] = {
    val activeSparkIndices = fields.indices.filter { i =>
      i == keyColumnIndex || !(ignoreNulls && !validateOnly && fields(i).dataType == NullType)
    }
    activeSparkIndices.zipWithIndex.map { case (sparkIdx, arrowIdx) =>
      val field = fields(sparkIdx)
      val vector = root.getVector(arrowIdx)
      val valueWriter = createValueWriter(field.dataType, vector)
      (row: InternalRow, targetRowIdx: Int) => valueWriter(row, sparkIdx, targetRowIdx)
    }.toArray
  }

  private val numActiveColumns: Int = columnWriters.length
  private var currentRowCount: Int = 0

  def reset(): Unit = {
    root.allocateNew()
    currentRowCount = 0
  }

  /**
   * Append a Spark `InternalRow` to the current Arrow batch.
   * Returns `true` if the row was appended, or `false` if `ignoreNulls = true` (and `!validateOnly`)
   * and all bin columns in the row are `null` (so the row is not sent over Arrow Flight).
   */
  def appendRow(row: InternalRow): Boolean = {
    if (row.isNullAt(keyColumnIndex)) {
      throw new IllegalArgumentException(
        s"Null value encountered in primary key column '$keyColumn' at batch row $currentRowCount"
      )
    }

    if (ignoreNulls && !validateOnly) {
      var hasNonNullBin = false
      var i = 0
      while (i < nonKeySparkOrdinals.length && !hasNonNullBin) {
        if (!row.isNullAt(nonKeySparkOrdinals(i))) {
          hasNonNullBin = true
        }
        i += 1
      }
      if (!hasNonNullBin) {
        return false
      }
    }

    var idx = 0
    while (idx < numActiveColumns) {
      columnWriters(idx)(row, currentRowCount)
      idx += 1
    }

    currentRowCount += 1
    true
  }

  def finishBatch(): Int = {
    val count = currentRowCount
    root.setRowCount(count)
    count
  }

  def rowCount: Int = currentRowCount

  /**
   * Pre-compiles a writer closure `(getter: SpecializedGetters, ordinal: Int, targetIndex: Int) => Unit`
   * for a specific `(DataType, FieldVector)` pair.
   */
  private def createValueWriter(
      dataType: DataType,
      vector: FieldVector
  ): (SpecializedGetters, Int, Int) => Unit = {
    dataType match {
      case NullType =>
        (_: SpecializedGetters, _: Int, _: Int) => ()

      case BooleanType =>
        val v = vector.asInstanceOf[BitVector]
        (getter, ordinal, targetIndex) =>
          if (getter.isNullAt(ordinal)) v.setNull(targetIndex)
          else v.setSafe(targetIndex, if (getter.getBoolean(ordinal)) 1 else 0)

      case ByteType =>
        val v = vector.asInstanceOf[TinyIntVector]
        (getter, ordinal, targetIndex) =>
          if (getter.isNullAt(ordinal)) v.setNull(targetIndex)
          else v.setSafe(targetIndex, getter.getByte(ordinal))

      case ShortType =>
        val v = vector.asInstanceOf[SmallIntVector]
        (getter, ordinal, targetIndex) =>
          if (getter.isNullAt(ordinal)) v.setNull(targetIndex)
          else v.setSafe(targetIndex, getter.getShort(ordinal))

      case IntegerType =>
        val v = vector.asInstanceOf[IntVector]
        (getter, ordinal, targetIndex) =>
          if (getter.isNullAt(ordinal)) v.setNull(targetIndex)
          else v.setSafe(targetIndex, getter.getInt(ordinal))

      case LongType =>
        val v = vector.asInstanceOf[BigIntVector]
        (getter, ordinal, targetIndex) =>
          if (getter.isNullAt(ordinal)) v.setNull(targetIndex)
          else v.setSafe(targetIndex, getter.getLong(ordinal))

      case FloatType =>
        val v = vector.asInstanceOf[Float4Vector]
        (getter, ordinal, targetIndex) =>
          if (getter.isNullAt(ordinal)) v.setNull(targetIndex)
          else v.setSafe(targetIndex, getter.getFloat(ordinal))

      case DoubleType =>
        val v = vector.asInstanceOf[Float8Vector]
        (getter, ordinal, targetIndex) =>
          if (getter.isNullAt(ordinal)) v.setNull(targetIndex)
          else v.setSafe(targetIndex, getter.getDouble(ordinal))

      case dt: DecimalType =>
        val v = vector.asInstanceOf[DecimalVector]
        val precision = dt.precision
        val scale = dt.scale
        (getter, ordinal, targetIndex) =>
          if (getter.isNullAt(ordinal)) v.setNull(targetIndex)
          else {
            val dec = getter.getDecimal(ordinal, precision, scale)
            if (dec == null) v.setNull(targetIndex)
            else v.setSafe(targetIndex, dec.toJavaBigDecimal)
          }

      case StringType =>
        val v = vector.asInstanceOf[VarCharVector]
        (getter, ordinal, targetIndex) =>
          if (getter.isNullAt(ordinal)) v.setNull(targetIndex)
          else {
            val utf8 = getter.getUTF8String(ordinal)
            if (utf8 == null) v.setNull(targetIndex)
            else v.setSafe(targetIndex, utf8.getBytes)
          }

      case BinaryType =>
        val v = vector.asInstanceOf[VarBinaryVector]
        (getter, ordinal, targetIndex) =>
          if (getter.isNullAt(ordinal)) v.setNull(targetIndex)
          else {
            val bytes = getter.getBinary(ordinal)
            if (bytes == null) v.setNull(targetIndex)
            else v.setSafe(targetIndex, bytes)
          }

      case DateType =>
        val v = vector.asInstanceOf[DateDayVector]
        (getter, ordinal, targetIndex) =>
          if (getter.isNullAt(ordinal)) v.setNull(targetIndex)
          else v.setSafe(targetIndex, getter.getInt(ordinal))

      case TimestampType =>
        val v = vector.asInstanceOf[TimeStampMicroTZVector]
        (getter, ordinal, targetIndex) =>
          if (getter.isNullAt(ordinal)) v.setNull(targetIndex)
          else v.setSafe(targetIndex, getter.getLong(ordinal))

      case ArrayType(elementType, _) =>
        val listVector = vector.asInstanceOf[ListVector]
        val elemWriter = createValueWriter(elementType, listVector.getDataVector)
        (getter, ordinal, targetIndex) =>
          if (getter.isNullAt(ordinal)) listVector.setNull(targetIndex)
          else {
            val arrayData = getter.getArray(ordinal)
            if (arrayData == null) listVector.setNull(targetIndex)
            else {
              val baseOffset: Int = listVector.startNewValue(targetIndex)
              val numElements = arrayData.numElements()
              var i = 0
              while (i < numElements) {
                elemWriter(arrayData, i, baseOffset + i)
                i += 1
              }
              listVector.endValue(targetIndex, numElements)
            }
          }

      case MapType(keyType, valueType, _) =>
        val mapVector = vector.asInstanceOf[MapVector]
        val entriesStruct = mapVector.getDataVector.asInstanceOf[StructVector]
        val keyVector = entriesStruct.getChild(MapVector.KEY_NAME)
        val valVector = entriesStruct.getChild(MapVector.VALUE_NAME)
        val keyWriter = createValueWriter(keyType, keyVector)
        val valWriter = createValueWriter(valueType, valVector)
        (getter, ordinal, targetIndex) =>
          if (getter.isNullAt(ordinal)) mapVector.setNull(targetIndex)
          else {
            val mapData = getter.getMap(ordinal)
            if (mapData == null) mapVector.setNull(targetIndex)
            else {
              val baseOffset: Int = mapVector.startNewValue(targetIndex)
              val keyArray = mapData.keyArray()
              val valArray = mapData.valueArray()
              val numEntries = mapData.numElements()
              var i = 0
              while (i < numEntries) {
                if (keyArray.isNullAt(i)) {
                  throw new IllegalArgumentException(s"Null map key encountered at row $targetIndex")
                }
                entriesStruct.setIndexDefined(baseOffset + i)
                keyWriter(keyArray, i, baseOffset + i)
                valWriter(valArray, i, baseOffset + i)
                i += 1
              }
              mapVector.endValue(targetIndex, numEntries)
            }
          }

      case StructType(structFields) =>
        val structVector = vector.asInstanceOf[StructVector]
        val numStructFields = structFields.length
        val childWriters = new Array[(SpecializedGetters, Int, Int) => Unit](numStructFields)
        var fIdx = 0
        while (fIdx < numStructFields) {
          val sf = structFields(fIdx)
          val childVec = structVector.getChildByOrdinal(fIdx).asInstanceOf[FieldVector]
          childWriters(fIdx) = createValueWriter(sf.dataType, childVec)
          fIdx += 1
        }
        (getter, ordinal, targetIndex) =>
          if (getter.isNullAt(ordinal)) structVector.setNull(targetIndex)
          else {
            val nestedRow = getter.getStruct(ordinal, numStructFields)
            if (nestedRow == null) structVector.setNull(targetIndex)
            else {
              structVector.setIndexDefined(targetIndex)
              var i = 0
              while (i < numStructFields) {
                childWriters(i)(nestedRow, i, targetIndex)
                i += 1
              }
            }
          }

      case other =>
        throw new UnsupportedOperationException(s"Unsupported Spark DataType: $other")
    }
  }
}
