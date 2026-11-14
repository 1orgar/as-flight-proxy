package com.asproxy.spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._

import java.nio.charset.StandardCharsets
import scala.collection.mutable

/**
 * Driver-side Format & Logical Control (ФЛК) validator for Spark DataFrames written to Aerospike.
 *
 * Enforces Aerospike 8+ constraints and Arrow Flight type compatibility at job initialization time
 * on the Spark Driver before any Executor tasks are launched:
 *  1. Namespace (1..31 bytes) and Set name (0..63 bytes) format & character rules.
 *  2. Primary key column presence and allowed Aerospike key types (String, Byte/Short/Int/Long, Binary).
 *  3. Bin count per record (1 .. maxBinsPerRecord, hard limit 32,767).
 *  4. Bin name format:
 *     - Non-empty, non-blank
 *     - UTF-8 byte length <= maxBinNameLen (Aerospike hard limit: 15 bytes)
 *     - Printable ASCII characters only (0x21..0x7E), no null bytes, control chars, whitespace, ':' or ';'
 *     - No duplicate bin names in the schema
 *  5. Recursive data type validation across primitive and nested CDT types (Array, Map, Struct).
 */
object AerospikeSchemaValidator extends Logging {

  /** Aerospike hard limit on bin name length in UTF-8 bytes (`AS_BIN_NAME_MAX_LEN - 1`). */
  val AerospikeHardMaxBinNameBytes: Int = 15

  /** Aerospike hard limit on number of bins in a single record. */
  val AerospikeHardMaxBinsPerRecord: Int = 32767

  /** Aerospike hard limit on namespace name length in UTF-8 bytes. */
  val AerospikeMaxNamespaceBytes: Int = 31

  /** Aerospike hard limit on set name length in UTF-8 bytes. */
  val AerospikeMaxSetBytes: Int = 63

  /**
   * Validate a DataFrame `StructType` and `AerospikeFlightOptions` on the Spark Driver.
   * Throws `IllegalArgumentException` with a complete list of ФЛК violations if any check fails.
   */
  def validate(schema: StructType, options: AerospikeFlightOptions): Unit = {
    validate(schema, options, requireWriteJobId = true)
  }

  def validate(
      schema: StructType,
      options: AerospikeFlightOptions,
      requireWriteJobId: Boolean
  ): Unit = {
    if (requireWriteJobId) {
      options.validateWriteTarget()
    } else {
      options.validateReadOrLookupTarget()
    }

    if (schema == null || schema.fields.isEmpty) {
      throw new IllegalArgumentException(
        "Aerospike ФЛК failed: DataFrame schema cannot be null or empty"
      )
    }

    val errors = mutable.ArrayBuffer.empty[String]
    val fields = schema.fields

    // 1. Validate Primary Key column presence and data type
    val keyFieldOpt = fields.find(_.name == options.keyColumn)
    keyFieldOpt match {
      case None =>
        val available = fields.map(_.name).mkString(", ")
        errors += s"Primary key column '${options.keyColumn}' not found in DataFrame schema (available columns: [$available])"
      case Some(keyField) =>
        if (!isSupportedPrimaryKeyType(keyField.dataType)) {
          errors += s"Primary key column '${keyField.name}' has unsupported data type '${keyField.dataType.simpleString}'. " +
            "Allowed Aerospike primary key types: string, tinyint, smallint, int, bigint, binary"
        }
    }

    // 2. Collect non-key (bin) columns and validate bin count
    val binFields = fields.filterNot(_.name == options.keyColumn)
    if (keyFieldOpt.isDefined && binFields.isEmpty) {
      errors += s"DataFrame must contain at least 1 bin column in addition to primary key column '${options.keyColumn}'"
    }
    if (options.shouldOmitNullsInFlight && binFields.nonEmpty && binFields.forall(_.dataType == NullType)) {
      errors += s"When 'aerospike.ignoreNulls' is true or 'aerospike.writeMode' is '${options.writeMode}', " +
        "DataFrame cannot consist solely of NullType (VOID) bin columns because null bins are omitted"
    }
    if (binFields.length > options.maxBinsPerRecord) {
      errors += s"DataFrame contains ${binFields.length} bin columns, which exceeds the maximum allowed bins per record (${options.maxBinsPerRecord})"
    }

    // 3. Validate each bin column name (length, character content, duplicates) and data type
    val seenBinNames = mutable.HashSet.empty[String]

    binFields.foreach { field =>
      val binName = field.name
      validateBinName(binName, options.maxBinNameLen, errors)

      if (binName != null && !seenBinNames.add(binName)) {
        errors += s"Duplicate bin name '$binName' in DataFrame schema"
      }

      validateDataType(field.dataType, s"bin '$binName'", errors)
    }

    if (errors.nonEmpty) {
      val formatted = errors.zipWithIndex
        .map { case (err, idx) => s"  [${idx + 1}] $err" }
        .mkString("\n")
      val msg =
        s"Aerospike ФЛК (Schema & Bin Validation) failed with ${errors.size} error(s):\n$formatted"
      logError(s"[Driver][ФЛК] $msg")
      throw new IllegalArgumentException(msg)
    }

    val keyTypeName = keyFieldOpt.map(_.dataType.simpleString).getOrElse("unknown")
    logInfo(
      s"[Driver][ФЛК] Schema validation passed for target='${options.namespace}.${options.setName}', " +
        s"keyColumn='${options.keyColumn}' ($keyTypeName), binsCount=${binFields.length}, " +
        s"writeMode='${options.writeMode}', omitNullsInFlight=${options.shouldOmitNullsInFlight}"
    )
  }

  /**
   * Validate Aerospike namespace and set names.
   */
  def validateNamespaceAndSet(namespace: String, setName: String): Unit = {
    val errors = mutable.ArrayBuffer.empty[String]

    if (namespace == null || namespace.trim.isEmpty) {
      errors += "'aerospike.namespace' cannot be empty"
    } else {
      val nsBytes = namespace.getBytes(StandardCharsets.UTF_8).length
      if (nsBytes > AerospikeMaxNamespaceBytes) {
        errors += s"Namespace '$namespace' ($nsBytes UTF-8 bytes) exceeds Aerospike limit of $AerospikeMaxNamespaceBytes bytes"
      }
      findInvalidIdentifierChar(namespace).foreach { reason =>
        errors += s"Namespace '$namespace' contains invalid character ($reason)"
      }
    }

    if (setName != null && setName.nonEmpty) {
      val setBytes = setName.getBytes(StandardCharsets.UTF_8).length
      if (setBytes > AerospikeMaxSetBytes) {
        errors += s"Set name '$setName' ($setBytes UTF-8 bytes) exceeds Aerospike limit of $AerospikeMaxSetBytes bytes"
      }
      findInvalidIdentifierChar(setName).foreach { reason =>
        errors += s"Set name '$setName' contains invalid character ($reason)"
      }
    }

    if (errors.nonEmpty) {
      throw new IllegalArgumentException(
        s"Aerospike ФЛК failed: ${errors.mkString("; ")}"
      )
    }
  }

  /**
   * Validate a single Aerospike bin name for non-emptiness, UTF-8 byte length, and allowed characters.
   */
  def validateBinName(
      binName: String,
      maxBinNameLen: Int,
      errors: mutable.Buffer[String]
  ): Unit = {
    if (binName == null || binName.isEmpty) {
      errors += "Bin name cannot be null or empty"
      return
    }
    if (binName.trim.isEmpty) {
      errors += s"Bin name '$binName' cannot consist only of whitespace"
      return
    }

    val utf8Bytes = binName.getBytes(StandardCharsets.UTF_8).length
    if (utf8Bytes > maxBinNameLen) {
      errors += s"Bin name '$binName' ($utf8Bytes UTF-8 bytes) exceeds Aerospike maximum bin name length ($maxBinNameLen bytes)"
    }

    findInvalidIdentifierChar(binName).foreach { reason =>
      errors += s"Bin name '$binName' has invalid format: $reason"
    }
  }

  /**
   * Check whether a Spark `DataType` is allowed as an Aerospike record Primary Key.
   */
  def isSupportedPrimaryKeyType(dataType: DataType): Boolean = dataType match {
    case StringType | ByteType | ShortType | IntegerType | LongType | BinaryType => true
    case _ => false
  }

  /**
   * Check whether a Spark `DataType` is allowed as a key inside an Aerospike CDT `MapType`.
   * Map keys in Aerospike CDT OrderedMap must be scalar, deterministic, non-floating-point types.
   */
  def isSupportedMapKeyType(dataType: DataType): Boolean = dataType match {
    case StringType | ByteType | ShortType | IntegerType | LongType | BinaryType |
         BooleanType | DateType | TimestampType | _: DecimalType =>
      true
    case _ => false
  }

  /**
   * Recursively validate a Spark `DataType` for compatibility with Arrow Flight and Aerospike bins/CDTs.
   */
  def validateDataType(
      dataType: DataType,
      fieldPath: String,
      errors: mutable.Buffer[String]
  ): Unit = dataType match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType |
         FloatType | DoubleType | StringType | BinaryType | DateType | TimestampType |
         NullType =>
      // Valid scalar types. NullType (VOID) is explicitly allowed because NULL is used to delete
      // bin values in Aerospike (Bin(name, Value::Nil)), including rows/schemas where all bins are NULL.

    case dt: DecimalType =>
      if (dt.precision < 1 || dt.precision > 38 || dt.scale < 0 || dt.scale > dt.precision) {
        errors += s"Invalid Decimal(${dt.precision}, ${dt.scale}) at $fieldPath: precision must be 1..38 and 0 <= scale <= precision"
      }

    case ArrayType(elementType, _) =>
      validateDataType(elementType, s"$fieldPath[]", errors)

    case MapType(keyType, valueType, _) =>
      if (!isSupportedMapKeyType(keyType)) {
        errors += s"Unsupported Map key type '${keyType.simpleString}' at $fieldPath{key}. " +
          "Allowed Aerospike CDT Map key types: string, tinyint, smallint, int, bigint, binary, boolean, date, timestamp, decimal"
      } else {
        validateDataType(keyType, s"$fieldPath{key}", errors)
      }
      validateDataType(valueType, s"$fieldPath{value}", errors)

    case StructType(nestedFields) =>
      if (nestedFields.isEmpty) {
        errors += s"Empty StructType (0 fields) is not allowed at $fieldPath"
      } else {
        val seenNested = mutable.HashSet.empty[String]
        nestedFields.foreach { sf =>
          if (sf.name == null || sf.name.trim.isEmpty) {
            errors += s"Nested struct field name cannot be empty at $fieldPath"
          } else {
            if (sf.name.indexOf('\u0000') >= 0) {
              errors += s"Nested struct field '${sf.name}' at $fieldPath contains forbidden null byte (\\u0000)"
            }
            if (!seenNested.add(sf.name)) {
              errors += s"Duplicate nested struct field '${sf.name}' at $fieldPath"
            }
            validateDataType(sf.dataType, s"$fieldPath.${sf.name}", errors)
          }
        }
      }

    case other =>
      errors += s"Unsupported Spark data type '${other.simpleString}' (${other.getClass.getSimpleName}) at $fieldPath"
  }

  /**
   * Inspect an Aerospike identifier (bin name, namespace, or set name) for forbidden characters:
   *  - null byte (`\u0000`)
   *  - whitespace (`' '`, `'\t'`, `'\n'`, `'\r'`)
   *  - ASCII control characters (`< 0x20` or `0x7F`)
   *  - Aerospike protocol delimiters (`':'`, `';'`)
   *  - non-ASCII characters (`> 0x7E`)
   */
  private def findInvalidIdentifierChar(name: String): Option[String] = {
    var i = 0
    while (i < name.length) {
      val c = name.charAt(i)
      if (c == '\u0000') {
        return Some(s"contains null byte (\\u0000) at index $i")
      }
      if (c.isWhitespace) {
        return Some(s"contains whitespace character at index $i")
      }
      if (c < 0x21 || c == 0x7F) {
        return Some(f"contains control character (0x${c.toInt}%02X) at index $i")
      }
      if (c == ':' || c == ';') {
        return Some(s"contains reserved Aerospike delimiter '$c' at index $i")
      }
      if (c > 0x7E) {
        return Some(s"contains non-ASCII character '$c' at index $i (only printable ASCII 0x21..0x7E is allowed)")
      }
      i += 1
    }
    None
  }
}
