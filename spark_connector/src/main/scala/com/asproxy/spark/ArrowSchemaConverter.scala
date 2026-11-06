package com.asproxy.spark

import org.apache.arrow.vector.complex.MapVector
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.types._

import java.util.Collections
import scala.collection.JavaConverters._

/**
 * Converts a Spark SQL `StructType` schema into an Apache Arrow `Schema`.
 * Supports primitive types, Date/Timestamp, Decimal, and nested Array/Map/Struct types.
 */
object ArrowSchemaConverter {

  def toArrowSchema(sparkSchema: StructType, timeZoneId: String = "UTC"): Schema = {
    val fields = sparkSchema.fields.map(f => toArrowField(f.name, f.dataType, f.nullable, timeZoneId))
    new Schema(fields.toList.asJava)
  }

  def toArrowField(
      name: String,
      dataType: DataType,
      nullable: Boolean,
      timeZoneId: String = "UTC"
  ): Field = {
    dataType match {
      case BooleanType =>
        new Field(name, new FieldType(nullable, ArrowType.Bool.INSTANCE, null), Collections.emptyList())

      case ByteType =>
        new Field(name, new FieldType(nullable, new ArrowType.Int(8, true), null), Collections.emptyList())

      case ShortType =>
        new Field(name, new FieldType(nullable, new ArrowType.Int(16, true), null), Collections.emptyList())

      case IntegerType =>
        new Field(name, new FieldType(nullable, new ArrowType.Int(32, true), null), Collections.emptyList())

      case LongType =>
        new Field(name, new FieldType(nullable, new ArrowType.Int(64, true), null), Collections.emptyList())

      case FloatType =>
        new Field(
          name,
          new FieldType(nullable, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null),
          Collections.emptyList()
        )

      case DoubleType =>
        new Field(
          name,
          new FieldType(nullable, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null),
          Collections.emptyList()
        )

      case dt: DecimalType =>
        new Field(
          name,
          new FieldType(nullable, new ArrowType.Decimal(dt.precision, dt.scale, 128), null),
          Collections.emptyList()
        )

      case StringType =>
        new Field(name, new FieldType(nullable, ArrowType.Utf8.INSTANCE, null), Collections.emptyList())

      case BinaryType =>
        new Field(name, new FieldType(nullable, ArrowType.Binary.INSTANCE, null), Collections.emptyList())

      case DateType =>
        new Field(
          name,
          new FieldType(nullable, new ArrowType.Date(DateUnit.DAY), null),
          Collections.emptyList()
        )

      case TimestampType =>
        new Field(
          name,
          new FieldType(nullable, new ArrowType.Timestamp(TimeUnit.MICROSECOND, timeZoneId), null),
          Collections.emptyList()
        )

      case ArrayType(elementType, containsNull) =>
        val childField = toArrowField("item", elementType, containsNull, timeZoneId)
        new Field(
          name,
          new FieldType(nullable, ArrowType.List.INSTANCE, null),
          Collections.singletonList(childField)
        )

      case MapType(keyType, valueType, valueContainsNull) =>
        // In Apache Arrow, a Map is represented as a List of non-nullable Struct("entries")
        // with non-nullable "key" and nullable/non-nullable "value" children.
        val keyField = toArrowField(MapVector.KEY_NAME, keyType, nullable = false, timeZoneId)
        val valField = toArrowField(MapVector.VALUE_NAME, valueType, valueContainsNull, timeZoneId)
        val entriesStruct = new Field(
          MapVector.DATA_VECTOR_NAME,
          new FieldType(false, ArrowType.Struct.INSTANCE, null),
          List(keyField, valField).asJava
        )
        new Field(
          name,
          new FieldType(nullable, new ArrowType.Map(false), null),
          Collections.singletonList(entriesStruct)
        )

      case StructType(structFields) =>
        val children = structFields
          .map(sf => toArrowField(sf.name, sf.dataType, sf.nullable, timeZoneId))
          .toList
          .asJava
        new Field(
          name,
          new FieldType(nullable, ArrowType.Struct.INSTANCE, null),
          children
        )

      case other =>
        throw new UnsupportedOperationException(
          s"Unsupported Spark DataType '$other' for column '$name'"
        )
    }
  }
}
