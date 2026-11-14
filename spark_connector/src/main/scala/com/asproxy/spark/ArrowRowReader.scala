package com.asproxy.spark

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, SpecificInternalRow}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Converts rows from an Apache Arrow `VectorSchemaRoot` into Spark Catalyst `InternalRow`s.
 *
 * Pre-compiles unboxed per-column `RowFieldReader`s once per `VectorSchemaRoot` and writes
 * primitives directly into a reusable `SpecificInternalRow` (`MutableInt`, `MutableLong`,
 * `MutableFloat`, `MutableDouble`, etc.) without per-cell JVM heap boxing or `DataType` matching.
 */
class ArrowRowReader(sparkSchema: StructType, root: VectorSchemaRoot) {

  private val fields: Array[StructField] = sparkSchema.fields
  private val numFields: Int = fields.length
  private val fieldReaders: Array[ArrowRowReader.RowFieldReader] = fields.map { f =>
    val vec = root.getVector(f.name)
    ArrowRowReader.createRowFieldReader(vec, f.dataType)
  }
  private val reusableRow: SpecificInternalRow = new SpecificInternalRow(sparkSchema)

  def readRow(rowIdx: Int): InternalRow = {
    var i = 0
    while (i < numFields) {
      fieldReaders(i).readInto(rowIdx, reusableRow, i)
      i += 1
    }
    reusableRow
  }
}

object ArrowRowReader {

  private val EmptyByteArray: Array[Byte] = Array.emptyByteArray

  /**
   * Unboxed field reader writing directly into `SpecificInternalRow` / `InternalRow` slots.
   */
  trait RowFieldReader {
    def readInto(rowIdx: Int, out: InternalRow, ordinal: Int): Unit
  }

  private val NullFieldReader: RowFieldReader = new RowFieldReader {
    override def readInto(rowIdx: Int, out: InternalRow, ordinal: Int): Unit = {
      out.setNullAt(ordinal)
    }
  }

  @inline private def readUtf8String(v: VarCharVector, idx: Int): UTF8String = {
    val offsetBuf = v.getOffsetBuffer
    val startOffset = offsetBuf.getInt(idx.toLong * BaseVariableWidthVector.OFFSET_WIDTH)
    val endOffset = offsetBuf.getInt((idx + 1).toLong * BaseVariableWidthVector.OFFSET_WIDTH)
    val len = endOffset - startOffset
    if (len <= 0) {
      UTF8String.EMPTY_UTF8
    } else {
      val bytes = new Array[Byte](len)
      v.getDataBuffer.getBytes(startOffset.toLong, bytes, 0, len)
      UTF8String.fromBytes(bytes)
    }
  }

  @inline private def readVarBinary(v: VarBinaryVector, idx: Int): Array[Byte] = {
    val offsetBuf = v.getOffsetBuffer
    val startOffset = offsetBuf.getInt(idx.toLong * BaseVariableWidthVector.OFFSET_WIDTH)
    val endOffset = offsetBuf.getInt((idx + 1).toLong * BaseVariableWidthVector.OFFSET_WIDTH)
    val len = endOffset - startOffset
    if (len <= 0) {
      EmptyByteArray
    } else {
      val bytes = new Array[Byte](len)
      v.getDataBuffer.getBytes(startOffset.toLong, bytes, 0, len)
      bytes
    }
  }

  /**
   * Pre-compiles an unboxed `RowFieldReader` for top-level and nested struct columns.
   */
  def createRowFieldReader(vector: FieldVector, dataType: DataType): RowFieldReader = {
    if (vector == null) {
      return NullFieldReader
    }

    dataType match {
      case NullType =>
        NullFieldReader

      case BooleanType =>
        val v = vector.asInstanceOf[BitVector]
        new RowFieldReader {
          override def readInto(rowIdx: Int, out: InternalRow, ordinal: Int): Unit = {
            if (v.isNull(rowIdx)) out.setNullAt(ordinal)
            else out.setBoolean(ordinal, v.get(rowIdx) != 0)
          }
        }

      case ByteType =>
        val v = vector.asInstanceOf[TinyIntVector]
        new RowFieldReader {
          override def readInto(rowIdx: Int, out: InternalRow, ordinal: Int): Unit = {
            if (v.isNull(rowIdx)) out.setNullAt(ordinal)
            else out.setByte(ordinal, v.get(rowIdx))
          }
        }

      case ShortType =>
        val v = vector.asInstanceOf[SmallIntVector]
        new RowFieldReader {
          override def readInto(rowIdx: Int, out: InternalRow, ordinal: Int): Unit = {
            if (v.isNull(rowIdx)) out.setNullAt(ordinal)
            else out.setShort(ordinal, v.get(rowIdx))
          }
        }

      case IntegerType =>
        vector match {
          case v: IntVector =>
            new RowFieldReader {
              override def readInto(rowIdx: Int, out: InternalRow, ordinal: Int): Unit = {
                if (v.isNull(rowIdx)) out.setNullAt(ordinal)
                else out.setInt(ordinal, v.get(rowIdx))
              }
            }
          case v: BigIntVector =>
            new RowFieldReader {
              override def readInto(rowIdx: Int, out: InternalRow, ordinal: Int): Unit = {
                if (v.isNull(rowIdx)) out.setNullAt(ordinal)
                else out.setInt(ordinal, v.get(rowIdx).toInt)
              }
            }
        }

      case DateType =>
        val v = vector.asInstanceOf[DateDayVector]
        new RowFieldReader {
          override def readInto(rowIdx: Int, out: InternalRow, ordinal: Int): Unit = {
            if (v.isNull(rowIdx)) out.setNullAt(ordinal)
            else out.setInt(ordinal, v.get(rowIdx))
          }
        }

      case LongType =>
        vector match {
          case v: BigIntVector =>
            new RowFieldReader {
              override def readInto(rowIdx: Int, out: InternalRow, ordinal: Int): Unit = {
                if (v.isNull(rowIdx)) out.setNullAt(ordinal)
                else out.setLong(ordinal, v.get(rowIdx))
              }
            }
          case v: IntVector =>
            new RowFieldReader {
              override def readInto(rowIdx: Int, out: InternalRow, ordinal: Int): Unit = {
                if (v.isNull(rowIdx)) out.setNullAt(ordinal)
                else out.setLong(ordinal, v.get(rowIdx).toLong)
              }
            }
        }

      case TimestampType =>
        vector match {
          case v: TimeStampMicroTZVector =>
            new RowFieldReader {
              override def readInto(rowIdx: Int, out: InternalRow, ordinal: Int): Unit = {
                if (v.isNull(rowIdx)) out.setNullAt(ordinal)
                else out.setLong(ordinal, v.get(rowIdx))
              }
            }
          case v: TimeStampMicroVector =>
            new RowFieldReader {
              override def readInto(rowIdx: Int, out: InternalRow, ordinal: Int): Unit = {
                if (v.isNull(rowIdx)) out.setNullAt(ordinal)
                else out.setLong(ordinal, v.get(rowIdx))
              }
            }
          case v: TimeStampMilliTZVector =>
            new RowFieldReader {
              override def readInto(rowIdx: Int, out: InternalRow, ordinal: Int): Unit = {
                if (v.isNull(rowIdx)) out.setNullAt(ordinal)
                else out.setLong(ordinal, v.get(rowIdx) * 1000L)
              }
            }
          case v: TimeStampMilliVector =>
            new RowFieldReader {
              override def readInto(rowIdx: Int, out: InternalRow, ordinal: Int): Unit = {
                if (v.isNull(rowIdx)) out.setNullAt(ordinal)
                else out.setLong(ordinal, v.get(rowIdx) * 1000L)
              }
            }
        }

      case FloatType =>
        vector match {
          case v: Float4Vector =>
            new RowFieldReader {
              override def readInto(rowIdx: Int, out: InternalRow, ordinal: Int): Unit = {
                if (v.isNull(rowIdx)) out.setNullAt(ordinal)
                else out.setFloat(ordinal, v.get(rowIdx))
              }
            }
          case v: Float8Vector =>
            new RowFieldReader {
              override def readInto(rowIdx: Int, out: InternalRow, ordinal: Int): Unit = {
                if (v.isNull(rowIdx)) out.setNullAt(ordinal)
                else out.setFloat(ordinal, v.get(rowIdx).toFloat)
              }
            }
        }

      case DoubleType =>
        vector match {
          case v: Float8Vector =>
            new RowFieldReader {
              override def readInto(rowIdx: Int, out: InternalRow, ordinal: Int): Unit = {
                if (v.isNull(rowIdx)) out.setNullAt(ordinal)
                else out.setDouble(ordinal, v.get(rowIdx))
              }
            }
          case v: Float4Vector =>
            new RowFieldReader {
              override def readInto(rowIdx: Int, out: InternalRow, ordinal: Int): Unit = {
                if (v.isNull(rowIdx)) out.setNullAt(ordinal)
                else out.setDouble(ordinal, v.get(rowIdx).toDouble)
              }
            }
        }

      case StringType =>
        val v = vector.asInstanceOf[VarCharVector]
        new RowFieldReader {
          override def readInto(rowIdx: Int, out: InternalRow, ordinal: Int): Unit = {
            if (v.isNull(rowIdx)) out.setNullAt(ordinal)
            else out.update(ordinal, readUtf8String(v, rowIdx))
          }
        }

      case BinaryType =>
        val v = vector.asInstanceOf[VarBinaryVector]
        new RowFieldReader {
          override def readInto(rowIdx: Int, out: InternalRow, ordinal: Int): Unit = {
            if (v.isNull(rowIdx)) out.setNullAt(ordinal)
            else out.update(ordinal, readVarBinary(v, rowIdx))
          }
        }

      case st @ StructType(structFields) =>
        val structVec = vector.asInstanceOf[StructVector]
        val numStructFields = structFields.length
        val childReaders: Array[RowFieldReader] = new Array[RowFieldReader](numStructFields)
        var fIdx = 0
        while (fIdx < numStructFields) {
          val sf = structFields(fIdx)
          val childByName = structVec.getChild(sf.name)
          val childVec = if (childByName != null) {
            childByName.asInstanceOf[FieldVector]
          } else {
            structVec.getChildByOrdinal(fIdx).asInstanceOf[FieldVector]
          }
          childReaders(fIdx) = createRowFieldReader(childVec, sf.dataType)
          fIdx += 1
        }
        val reusableStructRow = new SpecificInternalRow(st)
        new RowFieldReader {
          override def readInto(rowIdx: Int, out: InternalRow, ordinal: Int): Unit = {
            if (structVec.isNull(rowIdx)) {
              out.setNullAt(ordinal)
            } else {
              var i = 0
              while (i < numStructFields) {
                childReaders(i).readInto(rowIdx, reusableStructRow, i)
                i += 1
              }
              out.update(ordinal, reusableStructRow)
            }
          }
        }

      case other =>
        val boxedReader = createColumnReader(vector, other)
        new RowFieldReader {
          override def readInto(rowIdx: Int, out: InternalRow, ordinal: Int): Unit = {
            val value = boxedReader(rowIdx)
            if (value == null) out.setNullAt(ordinal)
            else out.update(ordinal, value)
          }
        }
    }
  }

  /**
   * Pre-compiles a specialized `Int => Any` reader function for a given `(FieldVector, DataType)` pair.
   */
  def createColumnReader(vector: FieldVector, dataType: DataType): Int => Any = {
    if (vector == null) {
      return (_: Int) => null
    }

    dataType match {
      case NullType =>
        (_: Int) => null

      case BooleanType =>
        val v = vector.asInstanceOf[BitVector]
        (idx: Int) => if (v.isNull(idx)) null else v.get(idx) != 0

      case ByteType =>
        val v = vector.asInstanceOf[TinyIntVector]
        (idx: Int) => if (v.isNull(idx)) null else v.get(idx)

      case ShortType =>
        val v = vector.asInstanceOf[SmallIntVector]
        (idx: Int) => if (v.isNull(idx)) null else v.get(idx)

      case IntegerType =>
        vector match {
          case v: IntVector =>
            (idx: Int) => if (v.isNull(idx)) null else v.get(idx)
          case v: BigIntVector =>
            (idx: Int) => if (v.isNull(idx)) null else v.get(idx).toInt
        }

      case LongType =>
        vector match {
          case v: BigIntVector =>
            (idx: Int) => if (v.isNull(idx)) null else v.get(idx)
          case v: IntVector =>
            (idx: Int) => if (v.isNull(idx)) null else v.get(idx).toLong
        }

      case FloatType =>
        vector match {
          case v: Float4Vector =>
            (idx: Int) => if (v.isNull(idx)) null else v.get(idx)
          case v: Float8Vector =>
            (idx: Int) => if (v.isNull(idx)) null else v.get(idx).toFloat
        }

      case DoubleType =>
        vector match {
          case v: Float8Vector =>
            (idx: Int) => if (v.isNull(idx)) null else v.get(idx)
          case v: Float4Vector =>
            (idx: Int) => if (v.isNull(idx)) null else v.get(idx).toDouble
        }

      case dt: DecimalType =>
        val v = vector.asInstanceOf[DecimalVector]
        val precision = dt.precision
        val scale = dt.scale
        (idx: Int) =>
          if (v.isNull(idx)) null
          else {
            val jbd = v.getObject(idx)
            if (jbd == null) null else Decimal(jbd, precision, scale)
          }

      case StringType =>
        val v = vector.asInstanceOf[VarCharVector]
        (idx: Int) => if (v.isNull(idx)) null else readUtf8String(v, idx)

      case BinaryType =>
        val v = vector.asInstanceOf[VarBinaryVector]
        (idx: Int) => if (v.isNull(idx)) null else readVarBinary(v, idx)

      case DateType =>
        val v = vector.asInstanceOf[DateDayVector]
        (idx: Int) => if (v.isNull(idx)) null else v.get(idx)

      case TimestampType =>
        vector match {
          case v: TimeStampMicroTZVector =>
            (idx: Int) => if (v.isNull(idx)) null else v.get(idx)
          case v: TimeStampMicroVector =>
            (idx: Int) => if (v.isNull(idx)) null else v.get(idx)
          case v: TimeStampMilliTZVector =>
            (idx: Int) => if (v.isNull(idx)) null else v.get(idx) * 1000L
          case v: TimeStampMilliVector =>
            (idx: Int) => if (v.isNull(idx)) null else v.get(idx) * 1000L
        }

      case ArrayType(elementType, _) =>
        val listVec = vector.asInstanceOf[ListVector]
        val elemReader = createColumnReader(listVec.getDataVector, elementType)
        (idx: Int) =>
          if (listVec.isNull(idx)) null
          else {
            val start = listVec.getElementStartIndex(idx)
            val end = listVec.getElementEndIndex(idx)
            val len = math.max(0, end - start)
            val arr = new Array[Any](len)
            var i = 0
            while (i < len) {
              arr(i) = elemReader(start + i)
              i += 1
            }
            new GenericArrayData(arr)
          }

      case MapType(keyType, valueType, _) =>
        val mapVec = vector.asInstanceOf[MapVector]
        val entriesStruct = mapVec.getDataVector.asInstanceOf[StructVector]
        val keyVec = entriesStruct.getChildByOrdinal(0).asInstanceOf[FieldVector]
        val valVec = entriesStruct.getChildByOrdinal(1).asInstanceOf[FieldVector]
        val keyReader = createColumnReader(keyVec, keyType)
        val valReader = createColumnReader(valVec, valueType)
        (idx: Int) =>
          if (mapVec.isNull(idx)) null
          else {
            val start = mapVec.getElementStartIndex(idx)
            val end = mapVec.getElementEndIndex(idx)
            val len = math.max(0, end - start)
            val keys = new Array[Any](len)
            val vals = new Array[Any](len)
            var i = 0
            while (i < len) {
              keys(i) = keyReader(start + i)
              vals(i) = valReader(start + i)
              i += 1
            }
            new ArrayBasedMapData(new GenericArrayData(keys), new GenericArrayData(vals))
          }

      case StructType(structFields) =>
        val structVec = vector.asInstanceOf[StructVector]
        val numStructFields = structFields.length
        val childReaders: Array[Int => Any] = new Array[Int => Any](numStructFields)
        var fIdx = 0
        while (fIdx < numStructFields) {
          val sf = structFields(fIdx)
          val childByName = structVec.getChild(sf.name)
          val childVec = if (childByName != null) {
            childByName.asInstanceOf[FieldVector]
          } else {
            structVec.getChildByOrdinal(fIdx).asInstanceOf[FieldVector]
          }
          childReaders(fIdx) = createColumnReader(childVec, sf.dataType)
          fIdx += 1
        }
        (idx: Int) =>
          if (structVec.isNull(idx)) null
          else {
            val rowValues = new Array[Any](numStructFields)
            var i = 0
            while (i < numStructFields) {
              rowValues(i) = childReaders(i)(idx)
              i += 1
            }
            new GenericInternalRow(rowValues)
          }

      case other =>
        throw new UnsupportedOperationException(
          s"Unsupported Spark DataType '$other' when reading from Arrow vector"
        )
    }
  }

  def readValue(vector: FieldVector, index: Int, dataType: DataType): Any = {
    createColumnReader(vector, dataType)(index)
  }
}
