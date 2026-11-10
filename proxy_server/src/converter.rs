use aerospike::{Bin, FloatValue, Key, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
    Float64Builder, Int8Builder, Int16Builder, Int32Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder, TimestampMillisecondBuilder, TimestampNanosecondBuilder,
    TimestampSecondBuilder,
};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Date64Type, Float16Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, Decimal128Array, Decimal256Array,
    FixedSizeBinaryArray, FixedSizeListArray, LargeBinaryArray, LargeListArray, LargeStringArray,
    ListArray, MapArray, NullArray, RecordBatch, RecordBatchOptions, StringArray, StringViewArray,
    StructArray, UInt32Array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_flight::{IpcMessage, SchemaAsIpc, SchemaResult};
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
use base64::Engine;
use bytes::Bytes;
use indexmap::IndexMap;

use crate::config::{LookupJoinType, TypeMismatchMode, WriteMode, WriteParameters};
use crate::error::{ProxyError, Result};
use crate::writer::RecordReadOutcome;

const MILLIS_PER_DAY: i64 = 86_400_000;

/// A single record read or scanned from Aerospike.
#[derive(Debug, Clone)]
pub struct ScannedRecord {
    pub key: Option<Key>,
    pub bins: IndexMap<String, Value>,
}

/// A single record converted from an Arrow `RecordBatch` row and ready to be written to Aerospike.
#[derive(Debug, Clone)]
pub struct PreparedRecord {
    pub key: Key,
    pub bins: Vec<Bin>,
}

/// Convert an entire Arrow `RecordBatch` into a vector of `PreparedRecord`s.
pub fn convert_record_batch(
    batch: &RecordBatch,
    params: &WriteParameters,
    max_bin_name_len: usize,
) -> Result<Vec<PreparedRecord>> {
    let num_rows = batch.num_rows();
    if num_rows == 0 {
        return Ok(Vec::new());
    }

    let schema = batch.schema();
    let key_col_idx = schema.index_of(&params.key_column).map_err(|_| {
        ProxyError::InvalidArgument(format!(
            "Key column '{}' not found in Arrow schema (available columns: {:?})",
            params.key_column,
            schema
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>()
        ))
    })?;

    let key_array = batch.column(key_col_idx);
    if key_array.null_count() > 0 {
        for row in 0..num_rows {
            if key_array.is_null(row) {
                return Err(ProxyError::InvalidArgument(format!(
                    "Null value encountered in primary key column '{}' at row {}",
                    params.key_column, row
                )));
            }
        }
    }

    // Collect non-key columns and validate bin name format, length, uniqueness, and count once per batch
    let mut bin_columns: Vec<(String, &dyn Array)> =
        Vec::with_capacity(schema.fields().len().saturating_sub(1));
    let mut seen_bin_names: HashSet<&str> =
        HashSet::with_capacity(schema.fields().len().saturating_sub(1));
    for (idx, field) in schema.fields().iter().enumerate() {
        if idx == key_col_idx {
            continue;
        }
        let bin_name = field.name();
        if bin_name.is_empty() {
            return Err(ProxyError::InvalidArgument(
                "Bin name cannot be empty".into(),
            ));
        }
        if bin_name.len() > max_bin_name_len {
            return Err(ProxyError::InvalidArgument(format!(
                "Bin name '{}' ({} bytes) exceeds configured max_bin_name_len ({})",
                bin_name,
                bin_name.len(),
                max_bin_name_len
            )));
        }
        if let Some(bad_char_err) = validate_bin_name_chars(bin_name) {
            return Err(ProxyError::InvalidArgument(format!(
                "Bin name '{}' has invalid format: {}",
                bin_name, bad_char_err
            )));
        }
        if !seen_bin_names.insert(bin_name.as_str()) {
            return Err(ProxyError::InvalidArgument(format!(
                "Duplicate bin name '{}' in Arrow schema",
                bin_name
            )));
        }
        bin_columns.push((bin_name.clone(), batch.column(idx).as_ref()));
    }

    if bin_columns.is_empty() {
        return Err(ProxyError::InvalidArgument(format!(
            "Arrow schema must contain at least 1 bin column in addition to primary key column '{}'",
            params.key_column
        )));
    }
    if bin_columns.len() > 32_767 {
        return Err(ProxyError::InvalidArgument(format!(
            "Arrow schema contains {} bin columns, which exceeds Aerospike's maximum of 32,767 bins per record",
            bin_columns.len()
        )));
    }

    // Determine whether null column values should be emitted as Value::Nil (to delete bins in update mode
    // or verify bin absence in DQ validate mode) or omitted.
    let is_validate = params.is_validate_mode();
    let emit_nil_for_nulls = if is_validate {
        !params.ignore_nulls
    } else {
        match params.write_mode {
            WriteMode::Update | WriteMode::UpdateOnly | WriteMode::Validate => !params.ignore_nulls,
            WriteMode::Replace | WriteMode::ReplaceOnly | WriteMode::CreateOnly => false,
        }
    };

    let mut records = Vec::with_capacity(num_rows);

    for row in 0..num_rows {
        let user_key_val = extract_key_value(key_array.as_ref(), row, &params.key_column)?;
        let key = Key::new(&params.namespace, &params.set_name, user_key_val).map_err(|e| {
            ProxyError::Conversion(format!("Failed to build Aerospike key at row {row}: {e}"))
        })?;

        let mut bins = Vec::with_capacity(bin_columns.len());
        for (bin_name, col_array) in &bin_columns {
            if col_array.is_null(row) || col_array.data_type() == &DataType::Null {
                if emit_nil_for_nulls {
                    bins.push(Bin::new(bin_name.clone(), Value::Nil));
                }
            } else {
                let val = extract_value(*col_array, row, !emit_nil_for_nulls)?;
                bins.push(Bin::new(bin_name.clone(), val));
            }
        }

        if is_validate || !bins.is_empty() {
            records.push(PreparedRecord { key, bins });
        }
    }

    Ok(records)
}

/// Result of comparing a single `PreparedRecord` from Spark against the record stored in Aerospike.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordDqResult {
    /// Key exists in Aerospike and all expected bins match (with `NaN == NaN` treated as `true`).
    Matched,
    /// Key was not found in Aerospike.
    MissingKey,
    /// Key exists in Aerospike, but at least one bin value or presence differed.
    BinMismatch(String),
}

/// Compare an expected `PreparedRecord` against the actual bins fetched from Aerospike.
pub fn validate_record_against_bins(
    expected: &PreparedRecord,
    actual_bins: Option<&HashMap<String, Value>>,
) -> RecordDqResult {
    let Some(actual_map) = actual_bins else {
        // When a row has PK and all bins set to NULL (Value::Nil with ignore_nulls = false),
        // deleting all bins of a record in Aerospike automatically removes the record if no
        // other bins remain. Therefore an absent record in Aerospike satisfies all-Nil bins.
        if !expected.bins.is_empty() && expected.bins.iter().all(|b| b.value == Value::Nil) {
            return RecordDqResult::Matched;
        }
        return RecordDqResult::MissingKey;
    };

    for bin in &expected.bins {
        let name = &bin.name;
        let expected_val = &bin.value;

        if *expected_val == Value::Nil {
            // When `ignore_nulls = false`, a null column in Spark deletes the bin in Aerospike,
            // so the bin must either be absent or Value::Nil.
            if let Some(actual_val) = actual_map.get(name) {
                if *actual_val != Value::Nil {
                    return RecordDqResult::BinMismatch(format!(
                        "bin '{name}': expected absent/Nil, actual {actual_val:?}"
                    ));
                }
            }
        } else {
            match actual_map.get(name) {
                None | Some(Value::Nil) => {
                    return RecordDqResult::BinMismatch(format!(
                        "bin '{name}': expected {expected_val:?}, actual missing/Nil"
                    ));
                }
                Some(actual_val) => {
                    if !values_equal_dq(expected_val, actual_val) {
                        return RecordDqResult::BinMismatch(format!(
                            "bin '{name}': expected {expected_val:?}, actual {actual_val:?}"
                        ));
                    }
                }
            }
        }
    }

    RecordDqResult::Matched
}

/// Recursive equality comparison for Aerospike `Value`s used in Data Quality (DQ) validation.
/// Explicitly treats comparing two `NaN` floats (`f64::is_nan()`) as `true`, including inside
/// nested `List`, `HashMap`, and `OrderedMap` structures.
pub fn values_equal_dq(expected: &Value, actual: &Value) -> bool {
    match (expected, actual) {
        (Value::Nil, Value::Nil) => true,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        // Some Aerospike setups return integer 0/1 for booleans if stored as int
        (Value::Bool(a), Value::Int(b)) | (Value::Int(b), Value::Bool(a)) => {
            (*a && *b == 1) || (!*a && *b == 0)
        }
        (Value::Int(a), Value::Int(b)) => a == b,
        (Value::Float(a), Value::Float(b)) => {
            let fa = f64::from(a.clone());
            let fb = f64::from(b.clone());
            (fa.is_nan() && fb.is_nan()) || (fa == fb)
        }
        (Value::String(a), Value::String(b)) => a == b,
        (Value::Blob(a), Value::Blob(b)) | (Value::Hll(a), Value::Hll(b)) => a == b,
        (Value::List(a), Value::List(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b.iter())
                    .all(|(x, y)| values_equal_dq(x, y))
        }
        (Value::OrderedMap(a), Value::OrderedMap(b)) => {
            a.len() == b.len()
                && a.iter().all(|(k_a, v_a)| {
                    b.get(k_a)
                        .or_else(|| {
                            b.iter()
                                .find(|(k_b, _)| values_equal_dq(k_a, k_b))
                                .map(|(_, v_b)| v_b)
                        })
                        .is_some_and(|v_b| values_equal_dq(v_a, v_b))
                })
        }
        (Value::HashMap(a), Value::HashMap(b)) => {
            a.len() == b.len()
                && a.iter().all(|(k_a, v_a)| {
                    b.get(k_a)
                        .or_else(|| {
                            b.iter()
                                .find(|(k_b, _)| values_equal_dq(k_a, k_b))
                                .map(|(_, v_b)| v_b)
                        })
                        .is_some_and(|v_b| values_equal_dq(v_a, v_b))
                })
        }
        (Value::OrderedMap(om), Value::HashMap(hm))
        | (Value::HashMap(hm), Value::OrderedMap(om)) => {
            om.len() == hm.len()
                && om.iter().all(|(k_a, v_a)| {
                    hm.get(k_a)
                        .or_else(|| {
                            hm.iter()
                                .find(|(k_b, _)| values_equal_dq(k_a, k_b))
                                .map(|(_, v_b)| v_b)
                        })
                        .is_some_and(|v_b| values_equal_dq(v_a, v_b))
                })
        }
        (a, b) => a == b,
    }
}

/// Extract a primary key `Value` (must be String, Int, or Blob in Aerospike).
fn extract_key_value(array: &dyn Array, row: usize, col_name: &str) -> Result<Value> {
    match array.data_type() {
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| ProxyError::Conversion("Failed to downcast Utf8 key".into()))?;
            Ok(Value::String(arr.value(row).to_string()))
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| ProxyError::Conversion("Failed to downcast LargeUtf8 key".into()))?;
            Ok(Value::String(arr.value(row).to_string()))
        }
        DataType::Utf8View => {
            let arr = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| ProxyError::Conversion("Failed to downcast Utf8View key".into()))?;
            Ok(Value::String(arr.value(row).to_string()))
        }
        DataType::Int8 => Ok(Value::Int(i64::from(array.as_primitive::<Int8Type>().value(row)))),
        DataType::Int16 => Ok(Value::Int(i64::from(
            array.as_primitive::<Int16Type>().value(row),
        ))),
        DataType::Int32 => Ok(Value::Int(i64::from(
            array.as_primitive::<Int32Type>().value(row),
        ))),
        DataType::Int64 => Ok(Value::Int(array.as_primitive::<Int64Type>().value(row))),
        DataType::UInt8 => Ok(Value::Int(i64::from(
            array.as_primitive::<UInt8Type>().value(row),
        ))),
        DataType::UInt16 => Ok(Value::Int(i64::from(
            array.as_primitive::<UInt16Type>().value(row),
        ))),
        DataType::UInt32 => Ok(Value::Int(i64::from(
            array.as_primitive::<UInt32Type>().value(row),
        ))),
        DataType::UInt64 => {
            let v = array.as_primitive::<UInt64Type>().value(row);
            let signed = i64::try_from(v).map_err(|_| {
                ProxyError::Conversion(format!(
                    "UInt64 key value {v} at row {row} overflows i64"
                ))
            })?;
            Ok(Value::Int(signed))
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| ProxyError::Conversion("Failed to downcast Binary key".into()))?;
            Ok(Value::Blob(arr.value(row).to_vec()))
        }
        DataType::LargeBinary => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| {
                    ProxyError::Conversion("Failed to downcast LargeBinary key".into())
                })?;
            Ok(Value::Blob(arr.value(row).to_vec()))
        }
        DataType::FixedSizeBinary(_) => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| {
                    ProxyError::Conversion("Failed to downcast FixedSizeBinary key".into())
                })?;
            Ok(Value::Blob(arr.value(row).to_vec()))
        }
        DataType::BinaryView => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryViewArray>()
                .ok_or_else(|| {
                    ProxyError::Conversion("Failed to downcast BinaryView key".into())
                })?;
            Ok(Value::Blob(arr.value(row).to_vec()))
        }
        other => Err(ProxyError::InvalidArgument(format!(
            "Unsupported primary key data type {:?} for column '{}'. Supported: String, Int8..64, UInt8..64, Binary",
            other, col_name
        ))),
    }
}

/// Extract an Aerospike `Value` from any supported Arrow `Array` at `row`.
/// Assumes `array.is_null(row)` has already been checked by the caller if needed,
/// though nested calls handle nulls by returning `Value::Nil`.
pub fn extract_value(array: &dyn Array, row: usize, ignore_nulls: bool) -> Result<Value> {
    if array.is_null(row) {
        return Ok(Value::Nil);
    }

    match array.data_type() {
        DataType::Null => Ok(Value::Nil),
        DataType::Boolean => {
            let arr = array.as_boolean();
            Ok(Value::Bool(arr.value(row)))
        }
        DataType::Int8 => Ok(Value::Int(i64::from(
            array.as_primitive::<Int8Type>().value(row),
        ))),
        DataType::Int16 => Ok(Value::Int(i64::from(
            array.as_primitive::<Int16Type>().value(row),
        ))),
        DataType::Int32 => Ok(Value::Int(i64::from(
            array.as_primitive::<Int32Type>().value(row),
        ))),
        DataType::Int64 => Ok(Value::Int(array.as_primitive::<Int64Type>().value(row))),
        DataType::UInt8 => Ok(Value::Int(i64::from(
            array.as_primitive::<UInt8Type>().value(row),
        ))),
        DataType::UInt16 => Ok(Value::Int(i64::from(
            array.as_primitive::<UInt16Type>().value(row),
        ))),
        DataType::UInt32 => Ok(Value::Int(i64::from(
            array.as_primitive::<UInt32Type>().value(row),
        ))),
        DataType::UInt64 => {
            let v = array.as_primitive::<UInt64Type>().value(row);
            Ok(Value::Int(v as i64))
        }
        DataType::Float16 => {
            let v = f64::from(array.as_primitive::<Float16Type>().value(row));
            Ok(Value::Float(FloatValue::from(v)))
        }
        DataType::Float32 => {
            let v = f64::from(array.as_primitive::<Float32Type>().value(row));
            Ok(Value::Float(FloatValue::from(v)))
        }
        DataType::Float64 => {
            let v = array.as_primitive::<Float64Type>().value(row);
            Ok(Value::Float(FloatValue::from(v)))
        }
        DataType::Decimal128(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| ProxyError::Conversion("Failed to downcast Decimal128".into()))?;
            Ok(Value::String(arr.value_as_string(row)))
        }
        DataType::Decimal256(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| ProxyError::Conversion("Failed to downcast Decimal256".into()))?;
            Ok(Value::String(arr.value_as_string(row)))
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| ProxyError::Conversion("Failed to downcast Utf8".into()))?;
            Ok(Value::String(arr.value(row).to_string()))
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| ProxyError::Conversion("Failed to downcast LargeUtf8".into()))?;
            Ok(Value::String(arr.value(row).to_string()))
        }
        DataType::Utf8View => {
            let arr = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| ProxyError::Conversion("Failed to downcast Utf8View".into()))?;
            Ok(Value::String(arr.value(row).to_string()))
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| ProxyError::Conversion("Failed to downcast Binary".into()))?;
            Ok(Value::Blob(arr.value(row).to_vec()))
        }
        DataType::LargeBinary => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| ProxyError::Conversion("Failed to downcast LargeBinary".into()))?;
            Ok(Value::Blob(arr.value(row).to_vec()))
        }
        DataType::FixedSizeBinary(_) => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| {
                    ProxyError::Conversion("Failed to downcast FixedSizeBinary".into())
                })?;
            Ok(Value::Blob(arr.value(row).to_vec()))
        }
        DataType::BinaryView => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryViewArray>()
                .ok_or_else(|| ProxyError::Conversion("Failed to downcast BinaryView".into()))?;
            Ok(Value::Blob(arr.value(row).to_vec()))
        }
        // Dates -> int64 UTC milliseconds
        DataType::Date32 => {
            let days = i64::from(array.as_primitive::<Date32Type>().value(row));
            Ok(Value::Int(days.saturating_mul(MILLIS_PER_DAY)))
        }
        DataType::Date64 => {
            let ms = array.as_primitive::<Date64Type>().value(row);
            Ok(Value::Int(ms))
        }
        // Timestamps -> int64 UTC milliseconds
        DataType::Timestamp(TimeUnit::Second, _) => {
            let secs = array.as_primitive::<TimestampSecondType>().value(row);
            Ok(Value::Int(secs.saturating_mul(1_000)))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let ms = array.as_primitive::<TimestampMillisecondType>().value(row);
            Ok(Value::Int(ms))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let micros = array.as_primitive::<TimestampMicrosecondType>().value(row);
            Ok(Value::Int(micros.div_euclid(1_000)))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let nanos = array.as_primitive::<TimestampNanosecondType>().value(row);
            Ok(Value::Int(nanos.div_euclid(1_000_000)))
        }
        // Nested List -> Aerospike CDT List
        DataType::List(_) => {
            let list_arr = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| ProxyError::Conversion("Failed to downcast ListArray".into()))?;
            let slice = list_arr.value(row);
            let mut items = Vec::with_capacity(slice.len());
            for i in 0..slice.len() {
                items.push(extract_value(slice.as_ref(), i, ignore_nulls)?);
            }
            Ok(Value::List(items))
        }
        DataType::LargeList(_) => {
            let list_arr = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| {
                    ProxyError::Conversion("Failed to downcast LargeListArray".into())
                })?;
            let slice = list_arr.value(row);
            let mut items = Vec::with_capacity(slice.len());
            for i in 0..slice.len() {
                items.push(extract_value(slice.as_ref(), i, ignore_nulls)?);
            }
            Ok(Value::List(items))
        }
        DataType::FixedSizeList(_, _) => {
            let list_arr = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| {
                    ProxyError::Conversion("Failed to downcast FixedSizeListArray".into())
                })?;
            let slice = list_arr.value(row);
            let mut items = Vec::with_capacity(slice.len());
            for i in 0..slice.len() {
                items.push(extract_value(slice.as_ref(), i, ignore_nulls)?);
            }
            Ok(Value::List(items))
        }
        // Nested Map -> Aerospike CDT OrderedMap
        DataType::Map(_, _) => {
            let map_arr = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| ProxyError::Conversion("Failed to downcast MapArray".into()))?;
            let entries: StructArray = map_arr.value(row);
            let keys_col = entries.column(0);
            let vals_col = entries.column(1);
            let mut map = IndexMap::with_capacity(entries.len());
            for i in 0..entries.len() {
                if keys_col.is_null(i) {
                    return Err(ProxyError::Conversion(format!(
                        "Null map key encountered in MapArray at row {row}, entry {i}"
                    )));
                }
                let k = extract_value(keys_col.as_ref(), i, ignore_nulls)?;
                let v = extract_value(vals_col.as_ref(), i, ignore_nulls)?;
                map.insert(k, v);
            }
            Ok(Value::OrderedMap(map))
        }
        // Nested Struct -> Aerospike CDT OrderedMap (field_name -> value)
        DataType::Struct(fields) => {
            let struct_arr = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| ProxyError::Conversion("Failed to downcast StructArray".into()))?;
            let mut map = IndexMap::with_capacity(fields.len());
            for (field_idx, field) in fields.iter().enumerate() {
                let child_col = struct_arr.column(field_idx);
                if child_col.is_null(row) && ignore_nulls {
                    continue;
                }
                let val = extract_value(child_col.as_ref(), row, ignore_nulls)?;
                map.insert(Value::String(field.name().clone()), val);
            }
            Ok(Value::OrderedMap(map))
        }
        other => Err(ProxyError::Conversion(format!(
            "Unsupported Arrow data type {:?} at row {}",
            other, row
        ))),
    }
}

fn validate_bin_name_chars(name: &str) -> Option<&'static str> {
    for b in name.bytes() {
        if b == 0 {
            return Some("contains null byte (\\0)");
        }
        if b.is_ascii_whitespace() {
            return Some("contains whitespace character");
        }
        if b < 0x21 || b == 0x7F {
            return Some("contains ASCII control character");
        }
        if b == b':' || b == b';' {
            return Some("contains reserved Aerospike delimiter (':' or ';')");
        }
        if b > 0x7E {
            return Some("contains non-ASCII character (only printable ASCII 0x21..0x7E is allowed)");
        }
    }
    None
}

/// Encode an Arrow `Schema` into `SchemaResult` (IPC framed schema bytes) and base64 string.
pub fn encode_arrow_schema_to_result(schema: &Schema) -> Result<SchemaResult> {
    let options = IpcWriteOptions::default();
    let schema_ipc = SchemaAsIpc::new(schema, &options);
    SchemaResult::try_from(schema_ipc)
        .map_err(|e| ProxyError::Conversion(format!("Failed to encode Arrow Schema to IPC: {e}")))
}

/// Encode an Arrow `Schema` into base64-encoded IPC bytes.
pub fn encode_arrow_schema_ipc_base64(schema: &Schema) -> Result<String> {
    let res = encode_arrow_schema_to_result(schema)?;
    Ok(base64::engine::general_purpose::STANDARD.encode(&res.schema))
}

/// Decode a base64-encoded Arrow IPC `Schema`.
pub fn decode_arrow_schema_ipc_base64(b64: &str) -> Result<Arc<Schema>> {
    let raw = base64::engine::general_purpose::STANDARD
        .decode(b64.trim())
        .map_err(|e| ProxyError::InvalidArgument(format!("Invalid base64 in schema_ipc_base64: {e}")))?;
    let schema = Schema::try_from(IpcMessage(Bytes::from(raw))).map_err(|e| {
        ProxyError::InvalidArgument(format!("Failed to decode Arrow Schema from IPC bytes: {e}"))
    })?;
    Ok(Arc::new(schema))
}

/// Extract Aerospike `Key`s from the specified `key_column` of an incoming Arrow `RecordBatch`.
pub fn extract_keys_from_batch(
    batch: &RecordBatch,
    namespace: &str,
    set_name: &str,
    key_column: &str,
) -> Result<Vec<Key>> {
    let num_rows = batch.num_rows();
    if num_rows == 0 {
        return Ok(Vec::new());
    }
    let schema = batch.schema();
    let key_col_idx = schema.index_of(key_column).map_err(|_| {
        ProxyError::InvalidArgument(format!(
            "Key column '{}' not found in lookup RecordBatch schema (available: {:?})",
            key_column,
            schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
        ))
    })?;

    let key_array = batch.column(key_col_idx);
    let mut keys = Vec::with_capacity(num_rows);
    for row in 0..num_rows {
        if key_array.is_null(row) {
            return Err(ProxyError::InvalidArgument(format!(
                "Null value encountered in primary key column '{key_column}' at row {row}"
            )));
        }
        let user_val = extract_key_value(key_array.as_ref(), row, key_column)?;
        let key = Key::new(namespace, set_name, user_val).map_err(|e| {
            ProxyError::Conversion(format!("Failed to build Aerospike key at row {row}: {e}"))
        })?;
        keys.push(key);
    }
    Ok(keys)
}

/// Infer an Arrow `Schema` from a sample of `ScannedRecord`s.
pub fn infer_arrow_schema_from_records(key_column: &str, records: &[ScannedRecord]) -> Schema {
    let mut key_type = DataType::Utf8;
    for rec in records {
        if let Some(ref k) = rec.key {
            if let Some(uk) = k.user_key() {
                key_type = match uk {
                    Value::Int(_) => DataType::Int64,
                    Value::Blob(_) => DataType::Binary,
                    _ => DataType::Utf8,
                };
                break;
            }
        }
    }

    let mut bin_types: IndexMap<String, Option<DataType>> = IndexMap::new();
    for rec in records {
        for (bin_name, val) in &rec.bins {
            if bin_name == key_column {
                continue;
            }
            let entry = bin_types.entry(bin_name.clone()).or_insert(None);
            if entry.is_none() {
                if let Some(inferred) = infer_arrow_type_from_value(val) {
                    *entry = Some(inferred);
                }
            }
        }
    }

    let mut fields = Vec::with_capacity(1 + bin_types.len());
    fields.push(Field::new(key_column, key_type, false));
    for (bin_name, dt_opt) in bin_types {
        fields.push(Field::new(
            bin_name,
            dt_opt.unwrap_or(DataType::Utf8),
            true,
        ));
    }

    Schema::new(fields)
}

fn infer_arrow_type_from_value(val: &Value) -> Option<DataType> {
    match val {
        Value::Nil => None,
        Value::Bool(_) => Some(DataType::Boolean),
        Value::Int(_) => Some(DataType::Int64),
        Value::Float(_) => Some(DataType::Float64),
        Value::String(_) | Value::GeoJson(_) => Some(DataType::Utf8),
        Value::Blob(_) | Value::Hll(_) => Some(DataType::Binary),
        Value::List(items) => {
            let mut elem_type = None;
            for item in items {
                if let Some(dt) = infer_arrow_type_from_value(item) {
                    elem_type = Some(dt);
                    break;
                }
            }
            let item_field = Field::new("element", elem_type.unwrap_or(DataType::Utf8), true);
            Some(DataType::List(Arc::new(item_field)))
        }
        Value::OrderedMap(entries) => Some(infer_map_data_type(entries.iter())),
        Value::HashMap(entries) => Some(infer_map_data_type(entries.iter())),
        _ => Some(DataType::Utf8),
    }
}

fn infer_map_data_type<'a, I: Iterator<Item = (&'a Value, &'a Value)>>(iter: I) -> DataType {
    let mut k_type = None;
    let mut v_type = None;
    for (k, v) in iter {
        if k_type.is_none() {
            k_type = infer_arrow_type_from_value(k);
        }
        if v_type.is_none() {
            v_type = infer_arrow_type_from_value(v);
        }
        if k_type.is_some() && v_type.is_some() {
            break;
        }
    }
    let entries_struct = DataType::Struct(Fields::from(vec![
        Field::new("keys", k_type.unwrap_or(DataType::Utf8), false),
        Field::new("values", v_type.unwrap_or(DataType::Utf8), true),
    ]));
    DataType::Map(
        Arc::new(Field::new("entries", entries_struct, false)),
        false,
    )
}

/// Convert a slice of `ScannedRecord`s into an Arrow `RecordBatch` matching `schema`.
pub fn records_to_record_batch(
    schema: &Arc<Schema>,
    key_column: &str,
    records: &[ScannedRecord],
    type_mismatch_mode: TypeMismatchMode,
) -> Result<RecordBatch> {
    let num_rows = records.len();
    if schema.fields().is_empty() {
        let opts = RecordBatchOptions::new().with_row_count(Some(num_rows));
        return RecordBatch::try_new_with_options(Arc::clone(schema), Vec::new(), &opts)
            .map_err(|e| ProxyError::Conversion(e.to_string()));
    }

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        if field.name() == key_column {
            let col = build_key_array_from_records(
                field.name(),
                field.data_type(),
                records,
                type_mismatch_mode,
            )?;
            columns.push(col);
        } else {
            let values: Vec<Option<&Value>> = records
                .iter()
                .map(|r| r.bins.get(field.name()))
                .collect();
            let col = build_array_from_values(
                field.name(),
                field.data_type(),
                &values,
                type_mismatch_mode,
            )?;
            columns.push(col);
        }
    }

    RecordBatch::try_new(Arc::clone(schema), columns)
        .map_err(|e| ProxyError::Conversion(e.to_string()))
}

/// Build an enriched Arrow `RecordBatch` for `do_exchange` by combining the incoming `input_batch`
/// columns with the requested `bins_schema` columns populated from Aerospike `outcomes`.
pub fn build_lookup_record_batch(
    input_batch: &RecordBatch,
    bins_schema: &Arc<Schema>,
    key_column: &str,
    outcomes: &[RecordReadOutcome],
    join_type: LookupJoinType,
    type_mismatch_mode: TypeMismatchMode,
) -> Result<Option<RecordBatch>> {
    let num_rows = input_batch.num_rows();
    if num_rows == 0 {
        return Ok(None);
    }

    let selected_indices: Vec<usize> = match join_type {
        LookupJoinType::Left => (0..num_rows).collect(),
        LookupJoinType::Inner => outcomes
            .iter()
            .enumerate()
            .filter_map(|(idx, outcome)| match outcome {
                RecordReadOutcome::Found(_) => Some(idx),
                _ => None,
            })
            .collect(),
    };

    if selected_indices.is_empty() {
        return Ok(None);
    }

    let input_schema = input_batch.schema();
    let mut out_fields: Vec<Arc<Field>> = input_schema.fields().iter().cloned().collect();
    let mut out_columns: Vec<ArrayRef> = Vec::new();

    // 1. Keep (or filter for inner join) all existing columns from input_batch
    if join_type == LookupJoinType::Left {
        for col in input_batch.columns() {
            out_columns.push(Arc::clone(col));
        }
    } else {
        let idx_arr = UInt32Array::from(
            selected_indices
                .iter()
                .map(|&i| i as u32)
                .collect::<Vec<u32>>(),
        );
        for col in input_batch.columns() {
            let taken = take_array_by_indices(col.as_ref(), &idx_arr)?;
            out_columns.push(taken);
        }
    }

    // 2. Append requested bin columns (skipping key_column if already present in input_batch)
    for bin_field in bins_schema.fields() {
        if bin_field.name() == key_column && input_schema.index_of(key_column).is_ok() {
            continue;
        }
        // Always nullable in Left join mode
        let nullable = bin_field.is_nullable() || join_type == LookupJoinType::Left;
        out_fields.push(Arc::new(
            bin_field.as_ref().clone().with_nullable(nullable),
        ));

        let values: Vec<Option<&Value>> = selected_indices
            .iter()
            .map(|&row_idx| match &outcomes[row_idx] {
                RecordReadOutcome::Found(map) => map.get(bin_field.name()),
                _ => None,
            })
            .collect();

        let arr = build_array_from_values(
            bin_field.name(),
            bin_field.data_type(),
            &values,
            type_mismatch_mode,
        )?;
        out_columns.push(arr);
    }

    let out_schema = Arc::new(Schema::new(out_fields));
    let batch = RecordBatch::try_new(out_schema, out_columns)
        .map_err(|e| ProxyError::Conversion(e.to_string()))?;
    Ok(Some(batch))
}

fn take_array_by_indices(array: &dyn Array, indices: &UInt32Array) -> Result<ArrayRef> {
    let row_indices: Vec<usize> = (0..indices.len())
        .map(|i| indices.value(i) as usize)
        .collect();
    let mut vals: Vec<Option<Value>> = Vec::with_capacity(row_indices.len());
    for &r in &row_indices {
        if array.is_null(r) {
            vals.push(None);
        } else {
            vals.push(Some(extract_value(array, r, false)?));
        }
    }
    let refs: Vec<Option<&Value>> = vals.iter().map(|v| v.as_ref()).collect();
    build_array_from_values("input_col", array.data_type(), &refs, TypeMismatchMode::Fail)
}

fn build_key_array_from_records(
    col_name: &str,
    data_type: &DataType,
    records: &[ScannedRecord],
    type_mismatch_mode: TypeMismatchMode,
) -> Result<ArrayRef> {
    let mut owned_fallback: Vec<Option<Value>> = Vec::with_capacity(records.len());
    for rec in records {
        match &rec.key {
            Some(k) => {
                if let Some(uk) = k.user_key() {
                    owned_fallback.push(Some(uk.clone()));
                } else {
                    // Record was stored with send_key=false; fall back to 20-byte digest
                    let digest = k.digest();
                    match data_type {
                        DataType::Binary => owned_fallback.push(Some(Value::Blob(digest.to_vec()))),
                        DataType::Utf8 => {
                            let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
                            owned_fallback.push(Some(Value::String(hex)));
                        }
                        _ => owned_fallback.push(None),
                    }
                }
            }
            None => owned_fallback.push(None),
        }
    }
    let refs: Vec<Option<&Value>> = owned_fallback.iter().map(|v| v.as_ref()).collect();
    build_array_from_values(col_name, data_type, &refs, type_mismatch_mode)
}

/// Build an Arrow `ArrayRef` of `data_type` from a slice of optional Aerospike `Value` references.
pub fn build_array_from_values(
    col_name: &str,
    data_type: &DataType,
    values: &[Option<&Value>],
    type_mismatch_mode: TypeMismatchMode,
) -> Result<ArrayRef> {
    let len = values.len();
    match data_type {
        DataType::Null => Ok(Arc::new(NullArray::new(len))),
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(len);
            for (row, val_opt) in values.iter().enumerate() {
                match val_opt {
                    None | Some(Value::Nil) => b.append_null(),
                    Some(Value::Bool(v)) => b.append_value(*v),
                    Some(Value::Int(0)) => b.append_value(false),
                    Some(Value::Int(1)) => b.append_value(true),
                    Some(other) => handle_mismatch(
                        col_name,
                        data_type,
                        other,
                        row,
                        type_mismatch_mode,
                        || b.append_null(),
                    )?,
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int8 => {
            let mut b = Int8Builder::with_capacity(len);
            for (row, val_opt) in values.iter().enumerate() {
                match val_opt {
                    None | Some(Value::Nil) => b.append_null(),
                    Some(Value::Int(v)) => match i8::try_from(*v) {
                        Ok(n) => b.append_value(n),
                        Err(_) => handle_mismatch(
                            col_name,
                            data_type,
                            &Value::Int(*v),
                            row,
                            type_mismatch_mode,
                            || b.append_null(),
                        )?,
                    },
                    Some(other) => handle_mismatch(
                        col_name,
                        data_type,
                        other,
                        row,
                        type_mismatch_mode,
                        || b.append_null(),
                    )?,
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int16 => {
            let mut b = Int16Builder::with_capacity(len);
            for (row, val_opt) in values.iter().enumerate() {
                match val_opt {
                    None | Some(Value::Nil) => b.append_null(),
                    Some(Value::Int(v)) => match i16::try_from(*v) {
                        Ok(n) => b.append_value(n),
                        Err(_) => handle_mismatch(
                            col_name,
                            data_type,
                            &Value::Int(*v),
                            row,
                            type_mismatch_mode,
                            || b.append_null(),
                        )?,
                    },
                    Some(other) => handle_mismatch(
                        col_name,
                        data_type,
                        other,
                        row,
                        type_mismatch_mode,
                        || b.append_null(),
                    )?,
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int32 => {
            let mut b = Int32Builder::with_capacity(len);
            for (row, val_opt) in values.iter().enumerate() {
                match val_opt {
                    None | Some(Value::Nil) => b.append_null(),
                    Some(Value::Int(v)) => match i32::try_from(*v) {
                        Ok(n) => b.append_value(n),
                        Err(_) => handle_mismatch(
                            col_name,
                            data_type,
                            &Value::Int(*v),
                            row,
                            type_mismatch_mode,
                            || b.append_null(),
                        )?,
                    },
                    Some(other) => handle_mismatch(
                        col_name,
                        data_type,
                        other,
                        row,
                        type_mismatch_mode,
                        || b.append_null(),
                    )?,
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(len);
            for (row, val_opt) in values.iter().enumerate() {
                match val_opt {
                    None | Some(Value::Nil) => b.append_null(),
                    Some(Value::Int(v)) => b.append_value(*v),
                    Some(other) => handle_mismatch(
                        col_name,
                        data_type,
                        other,
                        row,
                        type_mismatch_mode,
                        || b.append_null(),
                    )?,
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float32 => {
            let mut b = Float32Builder::with_capacity(len);
            for (row, val_opt) in values.iter().enumerate() {
                match val_opt {
                    None | Some(Value::Nil) => b.append_null(),
                    Some(Value::Float(f)) => b.append_value(f64::from(f.clone()) as f32),
                    Some(Value::Int(i)) => b.append_value(*i as f32),
                    Some(other) => handle_mismatch(
                        col_name,
                        data_type,
                        other,
                        row,
                        type_mismatch_mode,
                        || b.append_null(),
                    )?,
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(len);
            for (row, val_opt) in values.iter().enumerate() {
                match val_opt {
                    None | Some(Value::Nil) => b.append_null(),
                    Some(Value::Float(f)) => b.append_value(f64::from(f.clone())),
                    Some(Value::Int(i)) => b.append_value(*i as f64),
                    Some(other) => handle_mismatch(
                        col_name,
                        data_type,
                        other,
                        row,
                        type_mismatch_mode,
                        || b.append_null(),
                    )?,
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Decimal128(precision, scale) => {
            let mut b = Decimal128Builder::with_capacity(len)
                .with_data_type(DataType::Decimal128(*precision, *scale));
            for (row, val_opt) in values.iter().enumerate() {
                match val_opt {
                    None | Some(Value::Nil) => b.append_null(),
                    Some(Value::String(s)) => match parse_decimal128(s, *scale) {
                        Ok(scaled) => b.append_value(scaled),
                        Err(_) => handle_mismatch(
                            col_name,
                            data_type,
                            &Value::String(s.clone()),
                            row,
                            type_mismatch_mode,
                            || b.append_null(),
                        )?,
                    },
                    Some(Value::Int(i)) => {
                        let factor = 10_i128.pow((*scale).max(0) as u32);
                        b.append_value((*i as i128).saturating_mul(factor));
                    }
                    Some(other) => handle_mismatch(
                        col_name,
                        data_type,
                        other,
                        row,
                        type_mismatch_mode,
                        || b.append_null(),
                    )?,
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Utf8 => {
            let mut b = StringBuilder::with_capacity(len, len * 16);
            for (row, val_opt) in values.iter().enumerate() {
                match val_opt {
                    None | Some(Value::Nil) => b.append_null(),
                    Some(Value::String(s)) | Some(Value::GeoJson(s)) => b.append_value(s),
                    Some(other) => handle_mismatch(
                        col_name,
                        data_type,
                        other,
                        row,
                        type_mismatch_mode,
                        || b.append_null(),
                    )?,
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Binary => {
            let mut b = BinaryBuilder::with_capacity(len, len * 16);
            for (row, val_opt) in values.iter().enumerate() {
                match val_opt {
                    None | Some(Value::Nil) => b.append_null(),
                    Some(Value::Blob(bytes)) | Some(Value::Hll(bytes)) => b.append_value(bytes),
                    Some(other) => handle_mismatch(
                        col_name,
                        data_type,
                        other,
                        row,
                        type_mismatch_mode,
                        || b.append_null(),
                    )?,
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Date32 => {
            let mut b = Date32Builder::with_capacity(len);
            for (row, val_opt) in values.iter().enumerate() {
                match val_opt {
                    None | Some(Value::Nil) => b.append_null(),
                    Some(Value::Int(ms)) => {
                        let days = ms.div_euclid(MILLIS_PER_DAY) as i32;
                        b.append_value(days);
                    }
                    Some(other) => handle_mismatch(
                        col_name,
                        data_type,
                        other,
                        row,
                        type_mismatch_mode,
                        || b.append_null(),
                    )?,
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(len)
                .with_timezone_opt(tz.clone());
            for (row, val_opt) in values.iter().enumerate() {
                match val_opt {
                    None | Some(Value::Nil) => b.append_null(),
                    Some(Value::Int(ms)) => b.append_value(ms.saturating_mul(1_000)),
                    Some(other) => handle_mismatch(
                        col_name,
                        data_type,
                        other,
                        row,
                        type_mismatch_mode,
                        || b.append_null(),
                    )?,
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            let mut b = TimestampMillisecondBuilder::with_capacity(len)
                .with_timezone_opt(tz.clone());
            for (row, val_opt) in values.iter().enumerate() {
                match val_opt {
                    None | Some(Value::Nil) => b.append_null(),
                    Some(Value::Int(ms)) => b.append_value(*ms),
                    Some(other) => handle_mismatch(
                        col_name,
                        data_type,
                        other,
                        row,
                        type_mismatch_mode,
                        || b.append_null(),
                    )?,
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Timestamp(TimeUnit::Second, tz) => {
            let mut b = TimestampSecondBuilder::with_capacity(len).with_timezone_opt(tz.clone());
            for (row, val_opt) in values.iter().enumerate() {
                match val_opt {
                    None | Some(Value::Nil) => b.append_null(),
                    Some(Value::Int(ms)) => b.append_value(ms.div_euclid(1_000)),
                    Some(other) => handle_mismatch(
                        col_name,
                        data_type,
                        other,
                        row,
                        type_mismatch_mode,
                        || b.append_null(),
                    )?,
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            let mut b = TimestampNanosecondBuilder::with_capacity(len)
                .with_timezone_opt(tz.clone());
            for (row, val_opt) in values.iter().enumerate() {
                match val_opt {
                    None | Some(Value::Nil) => b.append_null(),
                    Some(Value::Int(ms)) => b.append_value(ms.saturating_mul(1_000_000)),
                    Some(other) => handle_mismatch(
                        col_name,
                        data_type,
                        other,
                        row,
                        type_mismatch_mode,
                        || b.append_null(),
                    )?,
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::List(item_field) => {
            let mut offsets: Vec<i32> = Vec::with_capacity(len + 1);
            offsets.push(0);
            let mut nulls: Vec<bool> = Vec::with_capacity(len);
            let mut child_vals: Vec<Option<&Value>> = Vec::new();

            for (row, val_opt) in values.iter().enumerate() {
                match val_opt {
                    None | Some(Value::Nil) => {
                        nulls.push(false);
                        offsets.push(*offsets.last().unwrap());
                    }
                    Some(Value::List(items)) => {
                        nulls.push(true);
                        for item in items {
                            if *item == Value::Nil {
                                child_vals.push(None);
                            } else {
                                child_vals.push(Some(item));
                            }
                        }
                        offsets.push(child_vals.len() as i32);
                    }
                    Some(other) => {
                        handle_mismatch(col_name, data_type, other, row, type_mismatch_mode, || {
                            nulls.push(false);
                            offsets.push(*offsets.last().unwrap());
                        })?;
                    }
                }
            }

            let child_col_name = format!("{col_name}[]");
            let child_arr = build_array_from_values(
                &child_col_name,
                item_field.data_type(),
                &child_vals,
                type_mismatch_mode,
            )?;
            let list_arr = ListArray::new(
                Arc::clone(item_field),
                OffsetBuffer::new(offsets.into()),
                child_arr,
                Some(NullBuffer::from(nulls)),
            );
            Ok(Arc::new(list_arr))
        }
        DataType::Map(entries_field, keys_sorted) => {
            let DataType::Struct(entry_fields) = entries_field.data_type() else {
                return Err(ProxyError::Conversion(format!(
                    "Invalid Map entries field type for '{col_name}': {:?}",
                    entries_field.data_type()
                )));
            };
            let key_field = &entry_fields[0];
            let val_field = &entry_fields[1];

            let mut offsets: Vec<i32> = Vec::with_capacity(len + 1);
            offsets.push(0);
            let mut nulls: Vec<bool> = Vec::with_capacity(len);
            let mut map_keys: Vec<Option<&Value>> = Vec::new();
            let mut map_vals: Vec<Option<&Value>> = Vec::new();

            for (row, val_opt) in values.iter().enumerate() {
                match val_opt {
                    None | Some(Value::Nil) => {
                        nulls.push(false);
                        offsets.push(*offsets.last().unwrap());
                    }
                    Some(Value::OrderedMap(entries)) => {
                        nulls.push(true);
                        for (k, v) in entries {
                            map_keys.push(Some(k));
                            if *v == Value::Nil {
                                map_vals.push(None);
                            } else {
                                map_vals.push(Some(v));
                            }
                        }
                        offsets.push(map_keys.len() as i32);
                    }
                    Some(Value::HashMap(entries)) => {
                        nulls.push(true);
                        for (k, v) in entries {
                            map_keys.push(Some(k));
                            if *v == Value::Nil {
                                map_vals.push(None);
                            } else {
                                map_vals.push(Some(v));
                            }
                        }
                        offsets.push(map_keys.len() as i32);
                    }
                    Some(other) => {
                        handle_mismatch(col_name, data_type, other, row, type_mismatch_mode, || {
                            nulls.push(false);
                            offsets.push(*offsets.last().unwrap());
                        })?;
                    }
                }
            }

            let keys_arr = build_array_from_values(
                &format!("{col_name}{{key}}"),
                key_field.data_type(),
                &map_keys,
                type_mismatch_mode,
            )?;
            let vals_arr = build_array_from_values(
                &format!("{col_name}{{value}}"),
                val_field.data_type(),
                &map_vals,
                type_mismatch_mode,
            )?;
            let entries_struct =
                StructArray::new(entry_fields.clone(), vec![keys_arr, vals_arr], None);
            let map_arr = MapArray::new(
                Arc::clone(entries_field),
                OffsetBuffer::new(offsets.into()),
                entries_struct,
                Some(NullBuffer::from(nulls)),
                *keys_sorted,
            );
            Ok(Arc::new(map_arr))
        }
        DataType::Struct(fields) => {
            let mut nulls: Vec<bool> = Vec::with_capacity(len);
            let mut per_field_vals: Vec<Vec<Option<&Value>>> =
                vec![Vec::with_capacity(len); fields.len()];
            let lookup_keys: Vec<Value> = fields
                .iter()
                .map(|f| Value::String(f.name().clone()))
                .collect();

            for (row, val_opt) in values.iter().enumerate() {
                match val_opt {
                    None | Some(Value::Nil) => {
                        nulls.push(false);
                        for fv in &mut per_field_vals {
                            fv.push(None);
                        }
                    }
                    Some(Value::OrderedMap(entries)) => {
                        nulls.push(true);
                        for (f_idx, lookup_key) in lookup_keys.iter().enumerate() {
                            match entries.get(lookup_key) {
                                None | Some(Value::Nil) => per_field_vals[f_idx].push(None),
                                Some(v) => per_field_vals[f_idx].push(Some(v)),
                            }
                        }
                    }
                    Some(Value::HashMap(entries)) => {
                        nulls.push(true);
                        for (f_idx, lookup_key) in lookup_keys.iter().enumerate() {
                            match entries.get(lookup_key) {
                                None | Some(Value::Nil) => per_field_vals[f_idx].push(None),
                                Some(v) => per_field_vals[f_idx].push(Some(v)),
                            }
                        }
                    }
                    Some(other) => {
                        handle_mismatch(col_name, data_type, other, row, type_mismatch_mode, || {
                            nulls.push(false);
                            for fv in &mut per_field_vals {
                                fv.push(None);
                            }
                        })?;
                    }
                }
            }

            let mut child_arrays: Vec<ArrayRef> = Vec::with_capacity(fields.len());
            for (f_idx, f) in fields.iter().enumerate() {
                let child_arr = build_array_from_values(
                    &format!("{col_name}.{}", f.name()),
                    f.data_type(),
                    &per_field_vals[f_idx],
                    type_mismatch_mode,
                )?;
                child_arrays.push(child_arr);
            }

            let struct_arr =
                StructArray::new(fields.clone(), child_arrays, Some(NullBuffer::from(nulls)));
            Ok(Arc::new(struct_arr))
        }
        other => Err(ProxyError::Conversion(format!(
            "Unsupported target Arrow DataType {other:?} for column '{col_name}'"
        ))),
    }
}

fn handle_mismatch<F: FnOnce()>(
    col_name: &str,
    expected_type: &DataType,
    actual_val: &Value,
    row: usize,
    mode: TypeMismatchMode,
    append_null_fn: F,
) -> Result<()> {
    match mode {
        TypeMismatchMode::Null => {
            append_null_fn();
            Ok(())
        }
        TypeMismatchMode::Fail => Err(ProxyError::Conversion(format!(
            "Type mismatch in column '{col_name}' at row {row}: expected Arrow type {expected_type:?}, but Aerospike bin contains {actual_val:?} (set aerospike.read.typeMismatchMode=null to return NULL instead)"
        ))),
    }
}

fn parse_decimal128(s: &str, scale: i8) -> std::result::Result<i128, ()> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err(());
    }
    let (neg, unsigned) = if let Some(rest) = trimmed.strip_prefix('-') {
        (true, rest)
    } else if let Some(rest) = trimmed.strip_prefix('+') {
        (false, rest)
    } else {
        (false, trimmed)
    };

    let (int_part, frac_part) = match unsigned.split_once('.') {
        Some((i, f)) => (i, f),
        None => (unsigned, ""),
    };

    let target_scale = scale.max(0) as usize;
    let mut digits = String::with_capacity(int_part.len() + target_scale);
    digits.push_str(int_part);
    if frac_part.len() >= target_scale {
        digits.push_str(&frac_part[..target_scale]);
    } else {
        digits.push_str(frac_part);
        for _ in 0..(target_scale - frac_part.len()) {
            digits.push('0');
        }
    }

    let parsed: i128 = digits.parse().map_err(|_| ())?;
    Ok(if neg { -parsed } else { parsed })
}
