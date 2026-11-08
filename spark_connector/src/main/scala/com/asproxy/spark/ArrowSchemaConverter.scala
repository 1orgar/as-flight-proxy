package com.asproxy.spark

import org.apache.arrow.vector.complex.MapVector
import org.apache.arrow.vector.ipc.WriteChannel
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.types._

import java.io.ByteArrayOutputStream
import java.nio.channels.Channels
import java.util.{Base64, Collections}
import scala.collection.JavaConverters._

/**
 * Converts between Spark SQL `StructType` schemas and Apache Arrow `Schema`s.
 * Supports primitive types, Date/Timestamp, Decimal, and nested Array/Map/Struct types.
 */
object ArrowSchemaConverter {

  def toArrowSchemaIpcBase64(sparkSchema: StructType, timeZoneId: String = "UTC"): String = {
    val arrowSchema = toArrowSchema(sparkSchema, timeZoneId)
    val baos = new ByteArrayOutputStream()
    val writeChannel = new WriteChannel(Channels.newChannel(baos))
    MessageSerializer.serialize(writeChannel, arrowSchema)
    Base64.getEncoder.encodeToString(baos.toByteArray)
  }

  def fromArrowSchema(arrowSchema: Schema): StructType = {
    val fields = arrowSchema.getFields.asScala.map(fromArrowField)
    StructType(fields)
  }

  def fromArrowField(field: Field): StructField = {
    val dt = fromArrowType(field)
    StructField(field.getName, dt, nullable = field.isNullable)
  }

  private def fromArrowType(field: Field): DataType = {
    val children = field.getChildren.asScala
    field.getType match {
      case _: ArrowType.Null => NullType
      case _: ArrowType.Bool => BooleanType
      case intType: ArrowType.Int =>
        intType.getBitWidth match {
          case 8 => ByteType
          case 16 => ShortType
          case 32 => IntegerType
          case 64 => LongType
          case other =>
            throw new UnsupportedOperationException(s"Unsupported Arrow Int bitWidth $other in field '${field.getName}'")
        }
      case fpType: ArrowType.FloatingPoint =>
        fpType.getPrecision match {
          case FloatingPointPrecision.SINGLE => FloatType
          case FloatingPointPrecision.DOUBLE => DoubleType
          case other =>
            throw new UnsupportedOperationException(s"Unsupported Arrow FloatingPoint precision $other in field '${field.getName}'")
        }
      case decType: ArrowType.Decimal =>
        DecimalType(decType.getPrecision, decType.getScale)
      case _: ArrowType.Utf8 | _: ArrowType.LargeUtf8 => StringType
      case _: ArrowType.Binary | _: ArrowType.LargeBinary | _: ArrowType.FixedSizeBinary => BinaryType
      case _: ArrowType.Date => DateType
      case _: ArrowType.Timestamp => TimestampType
      case _: ArrowType.List | _: ArrowType.LargeList | _: ArrowType.FixedSizeList =>
        if (children.isEmpty) {
          ArrayType(StringType, containsNull = true)
        } else {
          val elemField = children.head
          ArrayType(fromArrowType(elemField), containsNull = elemField.isNullable)
        }
      case _: ArrowType.Map =>
        if (children.isEmpty || children.head.getChildren.size() < 2) {
          MapType(StringType, StringType, valueContainsNull = true)
        } else {
          val entriesStruct = children.head
          val keyField = entriesStruct.getChildren.get(0)
          val valField = entriesStruct.getChildren.get(1)
          MapType(fromArrowType(keyField), fromArrowType(valField), valueContainsNull = valField.isNullable)
        }
      case _: ArrowType.Struct =>
        StructType(children.map(fromArrowField))
      case other =>
        throw new UnsupportedOperationException(
          s"Unsupported ArrowType '$other' in field '${field.getName}'"
        )
    }
  }

  def toArrowSchema(
      sparkSchema: StructType,
      timeZoneId: String = "UTC",
      keyColumn: String = "",
      ignoreNulls: Boolean = false,
      validateOnly: Boolean = false
  ): Schema = {
    val effectiveFields = if (ignoreNulls && !validateOnly) {
      sparkSchema.fields.filter(f => f.name == keyColumn || f.dataType != NullType)
    } else {
      sparkSchema.fields
    }
    val fields = effectiveFields.map(f => toArrowField(f.name, f.dataType, f.nullable, timeZoneId))
    new Schema(fields.toList.asJava)
  }

  def toArrowField(
      name: String,
      dataType: DataType,
      nullable: Boolean,
      timeZoneId: String = "UTC"
  ): Field = {
    dataType match {
      case NullType =>
        new Field(name, new FieldType(true, ArrowType.Null.INSTANCE, null), Collections.emptyList())

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
