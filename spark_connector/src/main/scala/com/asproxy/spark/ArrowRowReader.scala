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
 *
 * Pre-compiles per-column reader functions (`Int => Any`) once per `VectorSchemaRoot` to avoid
 * per-row/per-cell `DataType` pattern matching and `FieldVector` type casts on the hot read path,
 * and reuses a mutable `GenericInternalRow` across rows within a batch.
 */
class ArrowRowReader(sparkSchema: StructType, root: VectorSchemaRoot) {

  private val fields: Array[StructField] = sparkSchema.fields
  private val numFields: Int = fields.length
  private val columnReaders: Array[Int => Any] = fields.map { f =>
    val vec = root.getVector(f.name)
    ArrowRowReader.createColumnReader(vec, f.dataType)
  }
  private val reusableValues: Array[Any] = new Array[Any](numFields)
  private val reusableRow: GenericInternalRow = new GenericInternalRow(reusableValues)

  def readRow(rowIdx: Int): InternalRow = {
    var i = 0
    while (i < numFields) {
      reusableValues(i) = columnReaders(i)(rowIdx)
      i += 1
    }
    reusableRow
  }
}

object ArrowRowReader {

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
        (idx: Int) =>
          if (v.isNull(idx)) null
          else {
            val bytes = v.get(idx)
            if (bytes == null) null else UTF8String.fromBytes(bytes)
          }

      case BinaryType =>
        val v = vector.asInstanceOf[VarBinaryVector]
        (idx: Int) => if (v.isNull(idx)) null else v.get(idx)

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
