package com.asproxy.spark

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Converts rows from an Apache Arrow `VectorSchemaRoot` into Spark Catalyst `InternalRow`s.
 * Supports primitive types, Date/Timestamp, Decimal, and nested Array/Map/Struct types.
 */
class ArrowRowReader(sparkSchema: StructType, root: VectorSchemaRoot) {

  private val fields: Array[StructField] = sparkSchema.fields
  private val vectors: Array[FieldVector] = fields.map { f =>
    root.getVector(f.name)
  }

  def readRow(rowIdx: Int): InternalRow = {
    val values = new Array[Any](fields.length)
    var i = 0
    while (i < fields.length) {
      val vec = vectors(i)
      values(i) = if (vec == null) {
        null
      } else {
        ArrowRowReader.readValue(vec, rowIdx, fields(i).dataType)
      }
      i += 1
    }
    new GenericInternalRow(values)
  }
}

object ArrowRowReader {

  def readValue(vector: FieldVector, index: Int, dataType: DataType): Any = {
    if (vector == null || vector.isNull(index)) {
      return null
    }

    dataType match {
      case NullType =>
        null

      case BooleanType =>
        vector.asInstanceOf[BitVector].get(index) != 0

      case ByteType =>
        vector.asInstanceOf[TinyIntVector].get(index)

      case ShortType =>
        vector.asInstanceOf[SmallIntVector].get(index)

      case IntegerType =>
        vector match {
          case v: IntVector    => v.get(index)
          case v: BigIntVector => v.get(index).toInt
        }

      case LongType =>
        vector match {
          case v: BigIntVector => v.get(index)
          case v: IntVector    => v.get(index).toLong
        }

      case FloatType =>
        vector match {
          case v: Float4Vector => v.get(index)
          case v: Float8Vector => v.get(index).toFloat
        }

      case DoubleType =>
        vector match {
          case v: Float8Vector => v.get(index)
          case v: Float4Vector => v.get(index).toDouble
        }

      case dt: DecimalType =>
        val jbd = vector.asInstanceOf[DecimalVector].getObject(index)
        if (jbd == null) null else Decimal(jbd, dt.precision, dt.scale)

      case StringType =>
        val bytes = vector.asInstanceOf[VarCharVector].get(index)
        if (bytes == null) null else UTF8String.fromBytes(bytes)

      case BinaryType =>
        vector.asInstanceOf[VarBinaryVector].get(index)

      case DateType =>
        vector.asInstanceOf[DateDayVector].get(index)

      case TimestampType =>
        vector match {
          case v: TimeStampMicroTZVector => v.get(index)
          case v: TimeStampMicroVector   => v.get(index)
          case v: TimeStampMilliTZVector => v.get(index) * 1000L
          case v: TimeStampMilliVector   => v.get(index) * 1000L
        }

      case ArrayType(elementType, _) =>
        val listVec = vector.asInstanceOf[ListVector]
        val start = listVec.getElementStartIndex(index)
        val end = listVec.getElementEndIndex(index)
        val len = math.max(0, end - start)
        val dataVec = listVec.getDataVector
        val arr = new Array[Any](len)
        var i = 0
        while (i < len) {
          arr(i) = readValue(dataVec, start + i, elementType)
          i += 1
        }
        new GenericArrayData(arr)

      case MapType(keyType, valueType, _) =>
        val mapVec = vector.asInstanceOf[MapVector]
        val start = mapVec.getElementStartIndex(index)
        val end = mapVec.getElementEndIndex(index)
        val len = math.max(0, end - start)
        val entriesStruct = mapVec.getDataVector.asInstanceOf[StructVector]
        val keyVec = entriesStruct.getChildByOrdinal(0).asInstanceOf[FieldVector]
        val valVec = entriesStruct.getChildByOrdinal(1).asInstanceOf[FieldVector]
        val keys = new Array[Any](len)
        val vals = new Array[Any](len)
        var i = 0
        while (i < len) {
          keys(i) = readValue(keyVec, start + i, keyType)
          vals(i) = readValue(valVec, start + i, valueType)
          i += 1
        }
        new ArrayBasedMapData(new GenericArrayData(keys), new GenericArrayData(vals))

      case StructType(structFields) =>
        val structVec = vector.asInstanceOf[StructVector]
        val rowValues = new Array[Any](structFields.length)
        var fIdx = 0
        while (fIdx < structFields.length) {
          val sf = structFields(fIdx)
          val childByName = structVec.getChild(sf.name)
          val childVec = if (childByName != null) {
            childByName.asInstanceOf[FieldVector]
          } else {
            structVec.getChildByOrdinal(fIdx).asInstanceOf[FieldVector]
          }
          rowValues(fIdx) = readValue(childVec, index, sf.dataType)
          fIdx += 1
        }
        new GenericInternalRow(rowValues)

      case other =>
        throw new UnsupportedOperationException(
          s"Unsupported Spark DataType '$other' when reading from Arrow vector"
        )
    }
  }
}
