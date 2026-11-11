package com.asproxy.spark

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.types._

/**
 * Unboxed column writer for a top-level Spark `InternalRow` column into an Arrow `FieldVector`.
 * Avoids `scala.Function2` / `scala.Function3` primitive `Int` boxing on wide tables (100–300+ bins).
 */
private[spark] abstract class RowColumnWriter {
  def write(row: InternalRow, targetRowIdx: Int): Unit
}

/**
 * Unboxed indexed value writer for any `SpecializedGetters` (`InternalRow`, `ArrayData`) into an Arrow `FieldVector`.
 */
private[spark] abstract class IndexedValueWriter {
  def write(getter: SpecializedGetters, ordinal: Int, targetIndex: Int): Unit
}

/**
 * Populates an Apache Arrow `VectorSchemaRoot` from Spark Catalyst `InternalRow` records.
 *
 * Pre-compiles unboxed per-column writers once per `VectorSchemaRoot` so the hot `appendRow` loop
 * avoids per-cell `DataType` pattern matching, `FieldVector` casts, and `Int` boxing.
 */
class ArrowBatchConverter(
    sparkSchema: StructType,
    keyColumn: String,
    root: VectorSchemaRoot,
    ignoreNulls: Boolean = false,
    validateOnly: Boolean = false,
    initialCapacity: Int = 4096
) {

  private val effectiveInitialCapacity: Int = math.max(16, initialCapacity)
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

  // Pre-compiled unboxed per-column writers
  private val columnWriters: Array[RowColumnWriter] = {
    val activeSparkIndices = fields.indices.filter { i =>
      i == keyColumnIndex || !(ignoreNulls && !validateOnly && fields(i).dataType == NullType)
    }
    activeSparkIndices.zipWithIndex.map { case (sparkIdx, arrowIdx) =>
      val field = fields(sparkIdx)
      val vector = root.getVector(arrowIdx)
      vector.setInitialCapacity(effectiveInitialCapacity)
      val valueWriter = createValueWriter(field.dataType, vector)
      new RowColumnWriter {
        override def write(row: InternalRow, targetRowIdx: Int): Unit = {
          valueWriter.write(row, sparkIdx, targetRowIdx)
        }
      }
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

    val rowIdx = currentRowCount
    var idx = 0
    while (idx < numActiveColumns) {
      columnWriters(idx).write(row, rowIdx)
      idx += 1
    }

    currentRowCount = rowIdx + 1
    true
  }

  def finishBatch(): Int = {
    val count = currentRowCount
    root.setRowCount(count)
    count
  }

  def rowCount: Int = currentRowCount

  /**
   * Pre-compiles an unboxed `IndexedValueWriter` for a specific `(DataType, FieldVector)` pair.
   */
  private def createValueWriter(
      dataType: DataType,
      vector: FieldVector
  ): IndexedValueWriter = {
    dataType match {
      case NullType =>
        new IndexedValueWriter {
          override def write(getter: SpecializedGetters, ordinal: Int, targetIndex: Int): Unit = ()
        }

      case BooleanType =>
        val v = vector.asInstanceOf[BitVector]
        new IndexedValueWriter {
          override def write(getter: SpecializedGetters, ordinal: Int, targetIndex: Int): Unit =
            if (getter.isNullAt(ordinal)) v.setNull(targetIndex)
            else v.setSafe(targetIndex, if (getter.getBoolean(ordinal)) 1 else 0)
        }

      case ByteType =>
        val v = vector.asInstanceOf[TinyIntVector]
        new IndexedValueWriter {
          override def write(getter: SpecializedGetters, ordinal: Int, targetIndex: Int): Unit =
            if (getter.isNullAt(ordinal)) v.setNull(targetIndex)
            else v.setSafe(targetIndex, getter.getByte(ordinal))
        }

      case ShortType =>
        val v = vector.asInstanceOf[SmallIntVector]
        new IndexedValueWriter {
          override def write(getter: SpecializedGetters, ordinal: Int, targetIndex: Int): Unit =
            if (getter.isNullAt(ordinal)) v.setNull(targetIndex)
            else v.setSafe(targetIndex, getter.getShort(ordinal))
        }

      case IntegerType =>
        val v = vector.asInstanceOf[IntVector]
        new IndexedValueWriter {
          override def write(getter: SpecializedGetters, ordinal: Int, targetIndex: Int): Unit =
            if (getter.isNullAt(ordinal)) v.setNull(targetIndex)
            else v.setSafe(targetIndex, getter.getInt(ordinal))
        }

      case LongType =>
        val v = vector.asInstanceOf[BigIntVector]
        new IndexedValueWriter {
          override def write(getter: SpecializedGetters, ordinal: Int, targetIndex: Int): Unit =
            if (getter.isNullAt(ordinal)) v.setNull(targetIndex)
            else v.setSafe(targetIndex, getter.getLong(ordinal))
        }

      case FloatType =>
        val v = vector.asInstanceOf[Float4Vector]
        new IndexedValueWriter {
          override def write(getter: SpecializedGetters, ordinal: Int, targetIndex: Int): Unit =
            if (getter.isNullAt(ordinal)) v.setNull(targetIndex)
            else v.setSafe(targetIndex, getter.getFloat(ordinal))
        }

      case DoubleType =>
        val v = vector.asInstanceOf[Float8Vector]
        new IndexedValueWriter {
          override def write(getter: SpecializedGetters, ordinal: Int, targetIndex: Int): Unit =
            if (getter.isNullAt(ordinal)) v.setNull(targetIndex)
            else v.setSafe(targetIndex, getter.getDouble(ordinal))
        }

      case dt: DecimalType =>
        val v = vector.asInstanceOf[DecimalVector]
        val precision = dt.precision
        val scale = dt.scale
        new IndexedValueWriter {
          override def write(getter: SpecializedGetters, ordinal: Int, targetIndex: Int): Unit =
            if (getter.isNullAt(ordinal)) v.setNull(targetIndex)
            else {
              val dec = getter.getDecimal(ordinal, precision, scale)
              if (dec == null) v.setNull(targetIndex)
              else v.setSafe(targetIndex, dec.toJavaBigDecimal)
            }
        }

      case StringType =>
        val v = vector.asInstanceOf[VarCharVector]
        new IndexedValueWriter {
          override def write(getter: SpecializedGetters, ordinal: Int, targetIndex: Int): Unit =
            if (getter.isNullAt(ordinal)) v.setNull(targetIndex)
            else {
              val utf8 = getter.getUTF8String(ordinal)
              if (utf8 == null) v.setNull(targetIndex)
              else {
                val baseObj = utf8.getBaseObject
                if (baseObj.isInstanceOf[Array[Byte]]) {
                  val baseBytes = baseObj.asInstanceOf[Array[Byte]]
                  val byteOffset =
                    (utf8.getBaseOffset - org.apache.spark.unsafe.Platform.BYTE_ARRAY_OFFSET).toInt
                  v.setSafe(targetIndex, baseBytes, byteOffset, utf8.numBytes())
                } else {
                  v.setSafe(targetIndex, utf8.getBytes)
                }
              }
            }
        }

      case BinaryType =>
        val v = vector.asInstanceOf[VarBinaryVector]
        new IndexedValueWriter {
          override def write(getter: SpecializedGetters, ordinal: Int, targetIndex: Int): Unit =
            if (getter.isNullAt(ordinal)) v.setNull(targetIndex)
            else {
              val bytes = getter.getBinary(ordinal)
              if (bytes == null) v.setNull(targetIndex)
              else v.setSafe(targetIndex, bytes)
            }
        }

      case DateType =>
        val v = vector.asInstanceOf[DateDayVector]
        new IndexedValueWriter {
          override def write(getter: SpecializedGetters, ordinal: Int, targetIndex: Int): Unit =
            if (getter.isNullAt(ordinal)) v.setNull(targetIndex)
            else v.setSafe(targetIndex, getter.getInt(ordinal))
        }

      case TimestampType =>
        val v = vector.asInstanceOf[TimeStampMicroTZVector]
        new IndexedValueWriter {
          override def write(getter: SpecializedGetters, ordinal: Int, targetIndex: Int): Unit =
            if (getter.isNullAt(ordinal)) v.setNull(targetIndex)
            else v.setSafe(targetIndex, getter.getLong(ordinal))
        }

      case ArrayType(elementType, _) =>
        val listVector = vector.asInstanceOf[ListVector]
        val elemWriter = createValueWriter(elementType, listVector.getDataVector)
        new IndexedValueWriter {
          override def write(getter: SpecializedGetters, ordinal: Int, targetIndex: Int): Unit =
            if (getter.isNullAt(ordinal)) listVector.setNull(targetIndex)
            else {
              val arrayData = getter.getArray(ordinal)
              if (arrayData == null) listVector.setNull(targetIndex)
              else {
                val baseOffset: Int = listVector.startNewValue(targetIndex)
                val numElements = arrayData.numElements()
                var i = 0
                while (i < numElements) {
                  elemWriter.write(arrayData, i, baseOffset + i)
                  i += 1
                }
                listVector.endValue(targetIndex, numElements)
              }
            }
        }

      case MapType(keyType, valueType, _) =>
        val mapVector = vector.asInstanceOf[MapVector]
        val entriesStruct = mapVector.getDataVector.asInstanceOf[StructVector]
        val keyVector = entriesStruct.getChild(MapVector.KEY_NAME)
        val valVector = entriesStruct.getChild(MapVector.VALUE_NAME)
        val keyWriter = createValueWriter(keyType, keyVector)
        val valWriter = createValueWriter(valueType, valVector)
        new IndexedValueWriter {
          override def write(getter: SpecializedGetters, ordinal: Int, targetIndex: Int): Unit =
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
                  keyWriter.write(keyArray, i, baseOffset + i)
                  valWriter.write(valArray, i, baseOffset + i)
                  i += 1
                }
                mapVector.endValue(targetIndex, numEntries)
              }
            }
        }

      case StructType(structFields) =>
        val structVector = vector.asInstanceOf[StructVector]
        val numStructFields = structFields.length
        val childWriters = new Array[IndexedValueWriter](numStructFields)
        var fIdx = 0
        while (fIdx < numStructFields) {
          val sf = structFields(fIdx)
          val childVec = structVector.getChildByOrdinal(fIdx).asInstanceOf[FieldVector]
          childWriters(fIdx) = createValueWriter(sf.dataType, childVec)
          fIdx += 1
        }
        new IndexedValueWriter {
          override def write(getter: SpecializedGetters, ordinal: Int, targetIndex: Int): Unit =
            if (getter.isNullAt(ordinal)) structVector.setNull(targetIndex)
            else {
              val nestedRow = getter.getStruct(ordinal, numStructFields)
              if (nestedRow == null) structVector.setNull(targetIndex)
              else {
                structVector.setIndexDefined(targetIndex)
                var i = 0
                while (i < numStructFields) {
                  childWriters(i).write(nestedRow, i, targetIndex)
                  i += 1
                }
              }
            }
        }

      case other =>
        throw new UnsupportedOperationException(s"Unsupported Spark DataType: $other")
    }
  }
}
