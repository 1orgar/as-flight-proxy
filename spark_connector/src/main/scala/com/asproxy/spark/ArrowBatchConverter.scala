package com.asproxy.spark

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.types._

/**
 * Populates an Apache Arrow `VectorSchemaRoot` from Spark Catalyst `InternalRow` records.
 * Supports primitive types, Date/Timestamp, Decimal, and nested Array/Map/Struct types.
 */
class ArrowBatchConverter(
    sparkSchema: StructType,
    keyColumn: String,
    root: VectorSchemaRoot
) {

  private val fields: Array[StructField] = sparkSchema.fields
  private val keyColumnIndex: Int = {
    val idx = sparkSchema.fieldIndex(keyColumn)
    if (idx < 0) {
      throw new IllegalArgumentException(s"Key column '$keyColumn' not found in DataFrame schema")
    }
    idx
  }

  private val fieldVectors: Array[FieldVector] =
    fields.indices.map(i => root.getVector(i)).toArray

  private var currentRowCount: Int = 0

  def reset(): Unit = {
    root.allocateNew()
    currentRowCount = 0
  }

  def appendRow(row: InternalRow): Unit = {
    if (row.isNullAt(keyColumnIndex)) {
      throw new IllegalArgumentException(
        s"Null value encountered in primary key column '$keyColumn' at batch row $currentRowCount"
      )
    }

    var colIdx = 0
    while (colIdx < fields.length) {
      val field = fields(colIdx)
      val vector = fieldVectors(colIdx)
      writeValue(row, colIdx, field.dataType, vector, currentRowCount)
      colIdx += 1
    }

    currentRowCount += 1
  }

  def finishBatch(): Int = {
    val count = currentRowCount
    root.setRowCount(count)
    count
  }

  def rowCount: Int = currentRowCount

  private def writeValue(
      getter: SpecializedGetters,
      ordinal: Int,
      dataType: DataType,
      vector: FieldVector,
      targetIndex: Int
  ): Unit = {
    if (getter.isNullAt(ordinal)) {
      setNull(vector, targetIndex)
      return
    }

    dataType match {
      case BooleanType =>
        vector.asInstanceOf[BitVector].setSafe(targetIndex, if (getter.getBoolean(ordinal)) 1 else 0)

      case ByteType =>
        vector.asInstanceOf[TinyIntVector].setSafe(targetIndex, getter.getByte(ordinal))

      case ShortType =>
        vector.asInstanceOf[SmallIntVector].setSafe(targetIndex, getter.getShort(ordinal))

      case IntegerType =>
        vector.asInstanceOf[IntVector].setSafe(targetIndex, getter.getInt(ordinal))

      case LongType =>
        vector.asInstanceOf[BigIntVector].setSafe(targetIndex, getter.getLong(ordinal))

      case FloatType =>
        vector.asInstanceOf[Float4Vector].setSafe(targetIndex, getter.getFloat(ordinal))

      case DoubleType =>
        vector.asInstanceOf[Float8Vector].setSafe(targetIndex, getter.getDouble(ordinal))

      case dt: DecimalType =>
        val dec = getter.getDecimal(ordinal, dt.precision, dt.scale)
        if (dec == null) {
          vector.asInstanceOf[DecimalVector].setNull(targetIndex)
        } else {
          vector.asInstanceOf[DecimalVector].setSafe(targetIndex, dec.toJavaBigDecimal)
        }

      case StringType =>
        val utf8 = getter.getUTF8String(ordinal)
        if (utf8 == null) {
          vector.asInstanceOf[VarCharVector].setNull(targetIndex)
        } else {
          vector.asInstanceOf[VarCharVector].setSafe(targetIndex, utf8.getBytes)
        }

      case BinaryType =>
        val bytes = getter.getBinary(ordinal)
        if (bytes == null) {
          vector.asInstanceOf[VarBinaryVector].setNull(targetIndex)
        } else {
          vector.asInstanceOf[VarBinaryVector].setSafe(targetIndex, bytes)
        }

      case DateType =>
        // Spark DateType is days since Unix epoch; Rust proxy converts Date32 -> int64 UTC ms
        vector.asInstanceOf[DateDayVector].setSafe(targetIndex, getter.getInt(ordinal))

      case TimestampType =>
        // Spark TimestampType is microseconds since Unix epoch UTC; Rust proxy converts -> int64 UTC ms
        vector.asInstanceOf[TimeStampMicroTZVector].setSafe(targetIndex, getter.getLong(ordinal))

      case ArrayType(elementType, _) =>
        val listVector = vector.asInstanceOf[ListVector]
        val arrayData = getter.getArray(ordinal)
        if (arrayData == null) {
          listVector.setNull(targetIndex)
        } else {
          val baseOffset: Int = listVector.startNewValue(targetIndex)
          val dataVector = listVector.getDataVector
          val numElements = arrayData.numElements()
          var i = 0
          while (i < numElements) {
            writeValue(arrayData, i, elementType, dataVector, baseOffset + i)
            i += 1
          }
          listVector.endValue(targetIndex, numElements)
        }

      case MapType(keyType, valueType, _) =>
        val mapVector = vector.asInstanceOf[MapVector]
        val mapData = getter.getMap(ordinal)
        if (mapData == null) {
          mapVector.setNull(targetIndex)
        } else {
          val baseOffset: Int = mapVector.startNewValue(targetIndex)
          val entriesStruct = mapVector.getDataVector.asInstanceOf[StructVector]
          val keyVector = entriesStruct.getChild(MapVector.KEY_NAME)
          val valVector = entriesStruct.getChild(MapVector.VALUE_NAME)
          val keyArray = mapData.keyArray()
          val valArray = mapData.valueArray()
          val numEntries = mapData.numElements()
          var i = 0
          while (i < numEntries) {
            if (keyArray.isNullAt(i)) {
              throw new IllegalArgumentException(s"Null map key encountered at row $targetIndex")
            }
            entriesStruct.setIndexDefined(baseOffset + i)
            writeValue(keyArray, i, keyType, keyVector, baseOffset + i)
            writeValue(valArray, i, valueType, valVector, baseOffset + i)
            i += 1
          }
          mapVector.endValue(targetIndex, numEntries)
        }

      case StructType(structFields) =>
        val structVector = vector.asInstanceOf[StructVector]
        val nestedRow = getter.getStruct(ordinal, structFields.length)
        if (nestedRow == null) {
          structVector.setNull(targetIndex)
        } else {
          structVector.setIndexDefined(targetIndex)
          var fIdx = 0
          while (fIdx < structFields.length) {
            val sf = structFields(fIdx)
            val childVec = structVector.getChildByOrdinal(fIdx).asInstanceOf[FieldVector]
            writeValue(nestedRow, fIdx, sf.dataType, childVec, targetIndex)
            fIdx += 1
          }
        }

      case other =>
        throw new UnsupportedOperationException(s"Unsupported Spark DataType: $other")
    }
  }

  private def setNull(vector: FieldVector, targetIndex: Int): Unit = {
    vector match {
      case v: BaseFixedWidthVector    => v.setNull(targetIndex)
      case v: BaseVariableWidthVector => v.setNull(targetIndex)
      case v: ListVector              => v.setNull(targetIndex)
      case v: StructVector            => v.setNull(targetIndex)
      case _                          =>
    }
  }
}
