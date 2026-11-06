use aerospike::{Bin, FloatValue, Key, Value};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Date64Type, Float16Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, BinaryArray, BinaryViewArray, Decimal128Array, Decimal256Array, FixedSizeBinaryArray,
    FixedSizeListArray, LargeBinaryArray, LargeListArray, LargeStringArray, ListArray, MapArray,
    RecordBatch, StringArray, StringViewArray, StructArray,
};
use arrow_schema::{DataType, TimeUnit};
use indexmap::IndexMap;

use crate::config::{WriteMode, WriteParameters};
use crate::error::{ProxyError, Result};

const MILLIS_PER_DAY: i64 = 86_400_000;

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

    // Collect non-key columns and validate bin name lengths once per batch
    let mut bin_columns: Vec<(String, &dyn Array)> =
        Vec::with_capacity(schema.fields().len().saturating_sub(1));
    for (idx, field) in schema.fields().iter().enumerate() {
        if idx == key_col_idx {
            continue;
        }
        let bin_name = field.name();
        if bin_name.len() > max_bin_name_len {
            return Err(ProxyError::InvalidArgument(format!(
                "Bin name '{}' ({} bytes) exceeds configured max_bin_name_len ({})",
                bin_name,
                bin_name.len(),
                max_bin_name_len
            )));
        }
        bin_columns.push((bin_name.clone(), batch.column(idx).as_ref()));
    }

    // Determine whether null column values should be emitted as Value::Nil (to delete bins in update mode)
    // or omitted.
    let emit_nil_for_nulls = match params.write_mode {
        WriteMode::Update | WriteMode::UpdateOnly => !params.ignore_nulls,
        WriteMode::Replace | WriteMode::ReplaceOnly | WriteMode::CreateOnly => false,
    };

    let mut records = Vec::with_capacity(num_rows);

    for row in 0..num_rows {
        let user_key_val = extract_key_value(key_array.as_ref(), row, &params.key_column)?;
        let key = Key::new(&params.namespace, &params.set_name, user_key_val).map_err(|e| {
            ProxyError::Conversion(format!("Failed to build Aerospike key at row {row}: {e}"))
        })?;

        let mut bins = Vec::with_capacity(bin_columns.len());
        for (bin_name, col_array) in &bin_columns {
            if col_array.is_null(row) {
                if emit_nil_for_nulls {
                    bins.push(Bin::new(bin_name.clone(), Value::Nil));
                }
            } else {
                let val = extract_value(*col_array, row, params.ignore_nulls)?;
                bins.push(Bin::new(bin_name.clone(), val));
            }
        }

        // If all non-key columns were null in Replace mode (so `bins` is empty) and there is at least
        // one bin column in the schema, emit Nil for the first column so Aerospike receives a valid write op.
        if bins.is_empty() && !bin_columns.is_empty() && !params.ignore_nulls {
            bins.push(Bin::new(bin_columns[0].0.clone(), Value::Nil));
        }

        if !bins.is_empty() {
            records.push(PreparedRecord { key, bins });
        }
    }

    Ok(records)
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
