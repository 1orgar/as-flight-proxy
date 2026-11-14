use aerospike::{Bin, FloatValue, Key, Value};
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
    StructArray, make_array, new_empty_array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_flight::{IpcMessage, SchemaAsIpc, SchemaResult};
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
use base64::Engine;
use bytes::Bytes;
use indexmap::IndexMap;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

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

/// Pre-compiled primary key extractor that resolves Arrow `DataType` and downcasts once per `RecordBatch`
/// instead of once per row.
enum CompiledKeyColumn<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Utf8View(&'a StringViewArray),
    Int8(&'a [i8]),
    Int16(&'a [i16]),
    Int32(&'a [i32]),
    Int64(&'a [i64]),
    UInt8(&'a [u8]),
    UInt16(&'a [u16]),
    UInt32(&'a [u32]),
    UInt64(&'a [u64]),
    Binary(&'a BinaryArray),
    LargeBinary(&'a LargeBinaryArray),
    FixedSizeBinary(&'a FixedSizeBinaryArray),
    BinaryView(&'a BinaryViewArray),
}

impl<'a> CompiledKeyColumn<'a> {
    fn compile(array: &'a dyn Array, col_name: &str) -> Result<Self> {
        match array.data_type() {
            DataType::Utf8 => Ok(Self::Utf8(
                array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| ProxyError::Conversion("Failed to downcast Utf8 key".into()))?,
            )),
            DataType::LargeUtf8 => Ok(Self::LargeUtf8(
                array
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| {
                        ProxyError::Conversion("Failed to downcast LargeUtf8 key".into())
                    })?,
            )),
            DataType::Utf8View => Ok(Self::Utf8View(
                array
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .ok_or_else(|| {
                        ProxyError::Conversion("Failed to downcast Utf8View key".into())
                    })?,
            )),
            DataType::Int8 => Ok(Self::Int8(array.as_primitive::<Int8Type>().values())),
            DataType::Int16 => Ok(Self::Int16(array.as_primitive::<Int16Type>().values())),
            DataType::Int32 => Ok(Self::Int32(array.as_primitive::<Int32Type>().values())),
            DataType::Int64 => Ok(Self::Int64(array.as_primitive::<Int64Type>().values())),
            DataType::UInt8 => Ok(Self::UInt8(array.as_primitive::<UInt8Type>().values())),
            DataType::UInt16 => Ok(Self::UInt16(array.as_primitive::<UInt16Type>().values())),
            DataType::UInt32 => Ok(Self::UInt32(array.as_primitive::<UInt32Type>().values())),
            DataType::UInt64 => Ok(Self::UInt64(array.as_primitive::<UInt64Type>().values())),
            DataType::Binary => Ok(Self::Binary(
                array
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| {
                        ProxyError::Conversion("Failed to downcast Binary key".into())
                    })?,
            )),
            DataType::LargeBinary => Ok(Self::LargeBinary(
                array
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .ok_or_else(|| {
                        ProxyError::Conversion("Failed to downcast LargeBinary key".into())
                    })?,
            )),
            DataType::FixedSizeBinary(_) => Ok(Self::FixedSizeBinary(
                array
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .ok_or_else(|| {
                        ProxyError::Conversion("Failed to downcast FixedSizeBinary key".into())
                    })?,
            )),
            DataType::BinaryView => Ok(Self::BinaryView(
                array
                    .as_any()
                    .downcast_ref::<BinaryViewArray>()
                    .ok_or_else(|| {
                        ProxyError::Conversion("Failed to downcast BinaryView key".into())
                    })?,
            )),
            other => Err(ProxyError::InvalidArgument(format!(
                "Unsupported primary key data type {:?} for column '{}'. Supported: String, Int8..64, UInt8..64, Binary",
                other, col_name
            ))),
        }
    }

    #[inline]
    fn extract(&self, row: usize) -> Result<Value> {
        match self {
            Self::Utf8(arr) => Ok(Value::String(arr.value(row).to_string())),
            Self::LargeUtf8(arr) => Ok(Value::String(arr.value(row).to_string())),
            Self::Utf8View(arr) => Ok(Value::String(arr.value(row).to_string())),
            Self::Int8(vals) => Ok(Value::Int(i64::from(vals[row]))),
            Self::Int16(vals) => Ok(Value::Int(i64::from(vals[row]))),
            Self::Int32(vals) => Ok(Value::Int(i64::from(vals[row]))),
            Self::Int64(vals) => Ok(Value::Int(vals[row])),
            Self::UInt8(vals) => Ok(Value::Int(i64::from(vals[row]))),
            Self::UInt16(vals) => Ok(Value::Int(i64::from(vals[row]))),
            Self::UInt32(vals) => Ok(Value::Int(i64::from(vals[row]))),
            Self::UInt64(vals) => {
                let v = vals[row];
                let signed = i64::try_from(v).map_err(|_| {
                    ProxyError::Conversion(format!(
                        "UInt64 key value {v} at row {row} overflows i64"
                    ))
                })?;
                Ok(Value::Int(signed))
            }
            Self::Binary(arr) => Ok(Value::Blob(arr.value(row).to_vec())),
            Self::LargeBinary(arr) => Ok(Value::Blob(arr.value(row).to_vec())),
            Self::FixedSizeBinary(arr) => Ok(Value::Blob(arr.value(row).to_vec())),
            Self::BinaryView(arr) => Ok(Value::Blob(arr.value(row).to_vec())),
        }
    }
}

/// Pre-compiled bin column extractor that resolves Arrow `DataType`, typed value slices, and null buffers
/// once per `RecordBatch` so wide tables (100–300+ bins) avoid per-cell `DataType` matching and `Any` downcasting.
enum CompiledValueExtractor<'a> {
    Null,
    Boolean(&'a arrow_array::BooleanArray),
    Int8(&'a [i8]),
    Int16(&'a [i16]),
    Int32(&'a [i32]),
    Int64(&'a [i64]),
    UInt8(&'a [u8]),
    UInt16(&'a [u16]),
    UInt32(&'a [u32]),
    UInt64(&'a [u64]),
    Float32(&'a [f32]),
    Float64(&'a [f64]),
    Date32(&'a [i32]),
    Date64(&'a [i64]),
    TimestampSecond(&'a [i64]),
    TimestampMillisecond(&'a [i64]),
    TimestampMicrosecond(&'a [i64]),
    TimestampNanosecond(&'a [i64]),
    Utf8(&'a StringArray, bool),
    LargeUtf8(&'a LargeStringArray, bool),
    Utf8View(&'a StringViewArray, bool),
    Binary(&'a BinaryArray),
    LargeBinary(&'a LargeBinaryArray),
    FixedSizeBinary(&'a FixedSizeBinaryArray),
    BinaryView(&'a BinaryViewArray),
    Decimal128(&'a Decimal128Array),
    Decimal256(&'a Decimal256Array),
    Fallback(&'a dyn Array, bool, bool),
}

struct CompiledBinColumn<'a> {
    name: &'a str,
    nulls: Option<&'a NullBuffer>,
    extractor: CompiledValueExtractor<'a>,
}

impl<'a> CompiledBinColumn<'a> {
    fn compile(
        name: &'a str,
        array: &'a dyn Array,
        ignore_nulls: bool,
        auto_convert_strings: bool,
    ) -> Result<Self> {
        let nulls = if array.null_count() == 0 {
            None
        } else {
            array.nulls()
        };
        let extractor = match array.data_type() {
            DataType::Null => CompiledValueExtractor::Null,
            DataType::Boolean => CompiledValueExtractor::Boolean(array.as_boolean()),
            DataType::Int8 => {
                CompiledValueExtractor::Int8(array.as_primitive::<Int8Type>().values())
            }
            DataType::Int16 => {
                CompiledValueExtractor::Int16(array.as_primitive::<Int16Type>().values())
            }
            DataType::Int32 => {
                CompiledValueExtractor::Int32(array.as_primitive::<Int32Type>().values())
            }
            DataType::Int64 => {
                CompiledValueExtractor::Int64(array.as_primitive::<Int64Type>().values())
            }
            DataType::UInt8 => {
                CompiledValueExtractor::UInt8(array.as_primitive::<UInt8Type>().values())
            }
            DataType::UInt16 => {
                CompiledValueExtractor::UInt16(array.as_primitive::<UInt16Type>().values())
            }
            DataType::UInt32 => {
                CompiledValueExtractor::UInt32(array.as_primitive::<UInt32Type>().values())
            }
            DataType::UInt64 => {
                CompiledValueExtractor::UInt64(array.as_primitive::<UInt64Type>().values())
            }
            DataType::Float32 => {
                CompiledValueExtractor::Float32(array.as_primitive::<Float32Type>().values())
            }
            DataType::Float64 => {
                CompiledValueExtractor::Float64(array.as_primitive::<Float64Type>().values())
            }
            DataType::Date32 => {
                CompiledValueExtractor::Date32(array.as_primitive::<Date32Type>().values())
            }
            DataType::Date64 => {
                CompiledValueExtractor::Date64(array.as_primitive::<Date64Type>().values())
            }
            DataType::Timestamp(TimeUnit::Second, _) => CompiledValueExtractor::TimestampSecond(
                array.as_primitive::<TimestampSecondType>().values(),
            ),
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                CompiledValueExtractor::TimestampMillisecond(
                    array.as_primitive::<TimestampMillisecondType>().values(),
                )
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                CompiledValueExtractor::TimestampMicrosecond(
                    array.as_primitive::<TimestampMicrosecondType>().values(),
                )
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                CompiledValueExtractor::TimestampNanosecond(
                    array.as_primitive::<TimestampNanosecondType>().values(),
                )
            }
            DataType::Utf8 => CompiledValueExtractor::Utf8(
                array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| ProxyError::Conversion("Failed to downcast Utf8".into()))?,
                auto_convert_strings,
            ),
            DataType::LargeUtf8 => CompiledValueExtractor::LargeUtf8(
                array
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| ProxyError::Conversion("Failed to downcast LargeUtf8".into()))?,
                auto_convert_strings,
            ),
            DataType::Utf8View => CompiledValueExtractor::Utf8View(
                array
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .ok_or_else(|| ProxyError::Conversion("Failed to downcast Utf8View".into()))?,
                auto_convert_strings,
            ),
            DataType::Binary => CompiledValueExtractor::Binary(
                array
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| ProxyError::Conversion("Failed to downcast Binary".into()))?,
            ),
            DataType::LargeBinary => CompiledValueExtractor::LargeBinary(
                array
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .ok_or_else(|| {
                        ProxyError::Conversion("Failed to downcast LargeBinary".into())
                    })?,
            ),
            DataType::FixedSizeBinary(_) => CompiledValueExtractor::FixedSizeBinary(
                array
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .ok_or_else(|| {
                        ProxyError::Conversion("Failed to downcast FixedSizeBinary".into())
                    })?,
            ),
            DataType::BinaryView => CompiledValueExtractor::BinaryView(
                array
                    .as_any()
                    .downcast_ref::<BinaryViewArray>()
                    .ok_or_else(|| {
                        ProxyError::Conversion("Failed to downcast BinaryView".into())
                    })?,
            ),
            DataType::Decimal128(_, _) => CompiledValueExtractor::Decimal128(
                array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| {
                        ProxyError::Conversion("Failed to downcast Decimal128".into())
                    })?,
            ),
            DataType::Decimal256(_, _) => CompiledValueExtractor::Decimal256(
                array
                    .as_any()
                    .downcast_ref::<Decimal256Array>()
                    .ok_or_else(|| {
                        ProxyError::Conversion("Failed to downcast Decimal256".into())
                    })?,
            ),
            _ => CompiledValueExtractor::Fallback(array, ignore_nulls, auto_convert_strings),
        };
        Ok(Self {
            name,
            nulls,
            extractor,
        })
    }

    #[inline]
    fn is_null(&self, row: usize) -> bool {
        matches!(self.extractor, CompiledValueExtractor::Null)
            || self.nulls.is_some_and(|n| n.is_null(row))
    }

    #[inline]
    fn extract_non_null(&self, row: usize) -> Result<Value> {
        match self.extractor {
            CompiledValueExtractor::Null => Ok(Value::Nil),
            CompiledValueExtractor::Boolean(arr) => Ok(Value::Bool(arr.value(row))),
            CompiledValueExtractor::Int8(vals) => Ok(Value::Int(i64::from(vals[row]))),
            CompiledValueExtractor::Int16(vals) => Ok(Value::Int(i64::from(vals[row]))),
            CompiledValueExtractor::Int32(vals) => Ok(Value::Int(i64::from(vals[row]))),
            CompiledValueExtractor::Int64(vals) => Ok(Value::Int(vals[row])),
            CompiledValueExtractor::UInt8(vals) => Ok(Value::Int(i64::from(vals[row]))),
            CompiledValueExtractor::UInt16(vals) => Ok(Value::Int(i64::from(vals[row]))),
            CompiledValueExtractor::UInt32(vals) => Ok(Value::Int(i64::from(vals[row]))),
            CompiledValueExtractor::UInt64(vals) => {
                let v = i64::try_from(vals[row]).map_err(|_| {
                    ProxyError::Conversion(format!(
                        "UInt64 value {} in column '{}' at row {row} overflows i64::MAX",
                        vals[row], self.name
                    ))
                })?;
                Ok(Value::Int(v))
            }
            CompiledValueExtractor::Float32(vals) => {
                Ok(Value::Float(FloatValue::from(f64::from(vals[row]))))
            }
            CompiledValueExtractor::Float64(vals) => Ok(Value::Float(FloatValue::from(vals[row]))),
            CompiledValueExtractor::Date32(vals) => Ok(Value::Int(
                i64::from(vals[row]).saturating_mul(MILLIS_PER_DAY),
            )),
            CompiledValueExtractor::Date64(vals) => Ok(Value::Int(vals[row])),
            CompiledValueExtractor::TimestampSecond(vals) => {
                Ok(Value::Int(vals[row].saturating_mul(1_000)))
            }
            CompiledValueExtractor::TimestampMillisecond(vals) => Ok(Value::Int(vals[row])),
            CompiledValueExtractor::TimestampMicrosecond(vals) => {
                Ok(Value::Int(vals[row].div_euclid(1_000)))
            }
            CompiledValueExtractor::TimestampNanosecond(vals) => {
                Ok(Value::Int(vals[row].div_euclid(1_000_000)))
            }
            CompiledValueExtractor::Utf8(arr, auto_conv) => {
                Ok(parse_string_auto(arr.value(row), auto_conv))
            }
            CompiledValueExtractor::LargeUtf8(arr, auto_conv) => {
                Ok(parse_string_auto(arr.value(row), auto_conv))
            }
            CompiledValueExtractor::Utf8View(arr, auto_conv) => {
                Ok(parse_string_auto(arr.value(row), auto_conv))
            }
            CompiledValueExtractor::Binary(arr) => Ok(Value::Blob(arr.value(row).to_vec())),
            CompiledValueExtractor::LargeBinary(arr) => Ok(Value::Blob(arr.value(row).to_vec())),
            CompiledValueExtractor::FixedSizeBinary(arr) => {
                Ok(Value::Blob(arr.value(row).to_vec()))
            }
            CompiledValueExtractor::BinaryView(arr) => Ok(Value::Blob(arr.value(row).to_vec())),
            CompiledValueExtractor::Decimal128(arr) => Ok(Value::String(arr.value_as_string(row))),
            CompiledValueExtractor::Decimal256(arr) => Ok(Value::String(arr.value_as_string(row))),
            CompiledValueExtractor::Fallback(arr, ignore_nulls, auto_conv) => {
                extract_complex_value_with_options(arr, row, ignore_nulls, auto_conv)
            }
        }
    }
}

/// Validate the schema and primary key column of an Arrow `RecordBatch` once before slicing it into sub-batches.
/// Returns `key_col_idx`.
pub fn validate_record_batch_schema(
    batch: &RecordBatch,
    params: &WriteParameters,
    max_bin_name_len: usize,
) -> Result<usize> {
    let schema = batch.schema();
    let key_col_idx = schema.index_of(&params.key_column).map_err(|_| {
        ProxyError::InvalidArgument(format!(
            "Key column '{}' not found in Arrow schema (available columns: {:?})",
            params.key_column,
            schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
        ))
    })?;

    let num_rows = batch.num_rows();
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

    let num_bins = schema.fields().len().saturating_sub(1);
    if num_bins == 0 {
        return Err(ProxyError::InvalidArgument(format!(
            "Arrow schema must contain at least 1 bin column in addition to primary key column '{}'",
            params.key_column
        )));
    }
    if num_bins > 32_767 {
        return Err(ProxyError::InvalidArgument(format!(
            "Arrow schema contains {} bin columns, which exceeds Aerospike's maximum of 32,767 bins per record",
            num_bins
        )));
    }

    let mut seen_bin_names: HashSet<&str> = HashSet::with_capacity(num_bins);
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
    }

    Ok(key_col_idx)
}

/// Convert an Arrow `RecordBatch` (or slice) whose schema and primary key column were already validated.
pub fn convert_record_batch_with_key_index(
    batch: &RecordBatch,
    params: &WriteParameters,
    key_col_idx: usize,
) -> Result<Vec<PreparedRecord>> {
    let num_rows = batch.num_rows();
    if num_rows == 0 {
        return Ok(Vec::new());
    }

    let schema = batch.schema();
    let is_validate = params.is_validate_mode();
    let emit_nil_for_nulls = if is_validate {
        !params.ignore_nulls
    } else {
        match params.write_mode {
            WriteMode::Update | WriteMode::UpdateOnly | WriteMode::Validate => !params.ignore_nulls,
            WriteMode::Replace | WriteMode::ReplaceOnly | WriteMode::CreateOnly => false,
        }
    };

    let mut bin_columns: Vec<CompiledBinColumn<'_>> =
        Vec::with_capacity(schema.fields().len().saturating_sub(1));
    for (idx, field) in schema.fields().iter().enumerate() {
        if idx == key_col_idx {
            continue;
        }
        let compiled_col = CompiledBinColumn::compile(
            field.name().as_str(),
            batch.column(idx).as_ref(),
            !emit_nil_for_nulls,
            params.auto_convert_strings,
        )?;
        bin_columns.push(compiled_col);
    }

    let key_array = batch.column(key_col_idx);
    let compiled_key = CompiledKeyColumn::compile(key_array.as_ref(), &params.key_column)?;
    let mut records = Vec::with_capacity(num_rows);

    for row in 0..num_rows {
        let user_key_val = compiled_key.extract(row)?;
        let key = Key::new(&params.namespace, &params.set_name, user_key_val).map_err(|e| {
            ProxyError::Conversion(format!("Failed to build Aerospike key at row {row}: {e}"))
        })?;

        let mut bins = Vec::with_capacity(bin_columns.len());
        for col in &bin_columns {
            if col.is_null(row) {
                if emit_nil_for_nulls {
                    bins.push(Bin::new(col.name, Value::Nil));
                }
            } else {
                let val = col.extract_non_null(row)?;
                bins.push(Bin::new(col.name, val));
            }
        }

        if is_validate || !bins.is_empty() {
            records.push(PreparedRecord { key, bins });
        }
    }

    Ok(records)
}

/// Convert an entire Arrow `RecordBatch` into a vector of `PreparedRecord`s.
pub fn convert_record_batch(
    batch: &RecordBatch,
    params: &WriteParameters,
    max_bin_name_len: usize,
) -> Result<Vec<PreparedRecord>> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }
    let key_col_idx = validate_record_batch_schema(batch, params, max_bin_name_len)?;
    convert_record_batch_with_key_index(batch, params, key_col_idx)
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
            a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| values_equal_dq(x, y))
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

/// Convert a string slice into `Value::Int(i64)` or `Value::Float(FloatValue)` when
/// `auto_convert_strings` is enabled, falling back to `Value::String`.
/// Mirrors Python's `auto_type_convert(val, int, float)`:
/// 1. Try parsing as `i64` (`"1"` -> `Value::Int(1)`).
/// 2. Try parsing as finite `f64` (`"1.0"` -> `Value::Float(1.0)`).
/// 3. Fallback to `Value::String(s.to_string())`.
#[inline]
pub fn parse_string_auto(s: &str, auto_convert_strings: bool) -> Value {
    if auto_convert_strings {
        let trimmed = s.trim();
        if let Some(&first_byte) = trimmed.as_bytes().first() {
            if matches!(first_byte, b'0'..=b'9' | b'+' | b'-' | b'.') {
                if let Ok(i) = trimmed.parse::<i64>() {
                    return Value::Int(i);
                }
                if let Ok(f) = trimmed.parse::<f64>() {
                    if f.is_finite() {
                        return Value::Float(FloatValue::from(f));
                    }
                }
            }
        }
    }
    Value::String(s.to_string())
}

/// Extract an Aerospike `Value` from any supported Arrow `Array` at `row`.
/// Assumes `array.is_null(row)` has already been checked by the caller if needed,
/// though nested calls handle nulls by returning `Value::Nil`.
pub fn extract_value(array: &dyn Array, row: usize, ignore_nulls: bool) -> Result<Value> {
    extract_value_with_options(array, row, ignore_nulls, false)
}

/// Extract an Aerospike `Value` from any supported Arrow `Array` at `row`, optionally
/// auto-converting string values (`"1"` -> `Int(1)`, `"1.0"` -> `Float(1.0)`).
pub fn extract_value_with_options(
    array: &dyn Array,
    row: usize,
    ignore_nulls: bool,
    auto_convert_strings: bool,
) -> Result<Value> {
    if array.is_null(row) {
        return Ok(Value::Nil);
    }
    let compiled = CompiledBinColumn::compile("", array, ignore_nulls, auto_convert_strings)?;
    compiled.extract_non_null(row)
}

fn extract_complex_value_with_options(
    array: &dyn Array,
    row: usize,
    ignore_nulls: bool,
    auto_convert_strings: bool,
) -> Result<Value> {
    match array.data_type() {
        DataType::Float16 => {
            let v = f64::from(array.as_primitive::<Float16Type>().value(row));
            Ok(Value::Float(FloatValue::from(v)))
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
                items.push(extract_value_with_options(
                    slice.as_ref(),
                    i,
                    ignore_nulls,
                    auto_convert_strings,
                )?);
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
                items.push(extract_value_with_options(
                    slice.as_ref(),
                    i,
                    ignore_nulls,
                    auto_convert_strings,
                )?);
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
                items.push(extract_value_with_options(
                    slice.as_ref(),
                    i,
                    ignore_nulls,
                    auto_convert_strings,
                )?);
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
                let k = extract_value_with_options(keys_col.as_ref(), i, ignore_nulls, false)?;
                let v = extract_value_with_options(
                    vals_col.as_ref(),
                    i,
                    ignore_nulls,
                    auto_convert_strings,
                )?;
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
                let val = extract_value_with_options(
                    child_col.as_ref(),
                    row,
                    ignore_nulls,
                    auto_convert_strings,
                )?;
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
            return Some(
                "contains non-ASCII character (only printable ASCII 0x21..0x7E is allowed)",
            );
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
        .map_err(|e| {
            ProxyError::InvalidArgument(format!("Invalid base64 in schema_ipc_base64: {e}"))
        })?;
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
    if key_array.null_count() > 0 {
        for row in 0..num_rows {
            if key_array.is_null(row) {
                return Err(ProxyError::InvalidArgument(format!(
                    "Null value encountered in primary key column '{key_column}' at row {row}"
                )));
            }
        }
    }
    let compiled_key = CompiledKeyColumn::compile(key_array.as_ref(), key_column)?;
    let mut keys = Vec::with_capacity(num_rows);
    for row in 0..num_rows {
        let user_val = compiled_key.extract(row)?;
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
        fields.push(Field::new(bin_name, dt_opt.unwrap_or(DataType::Utf8), true));
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
            let values: Vec<Option<&Value>> =
                records.iter().map(|r| r.bins.get(field.name())).collect();
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

/// Build an Arrow `RecordBatch` directly from `BatchKeys` lookup `keys` and `outcomes`
/// without allocating intermediate `IndexMap` / `ScannedRecord` wrappers.
pub fn batch_outcomes_to_record_batch(
    schema: &Arc<Schema>,
    key_column: &str,
    keys: &[Key],
    outcomes: &[RecordReadOutcome],
    type_mismatch_mode: TypeMismatchMode,
) -> Result<Option<RecordBatch>> {
    let found: Vec<(&Key, &HashMap<String, Value>)> = keys
        .iter()
        .zip(outcomes.iter())
        .filter_map(|(k, o)| match o {
            RecordReadOutcome::Found(map) => Some((k, map)),
            _ => None,
        })
        .collect();

    if found.is_empty() {
        return Ok(None);
    }

    let num_rows = found.len();
    if schema.fields().is_empty() {
        let opts = RecordBatchOptions::new().with_row_count(Some(num_rows));
        let rb = RecordBatch::try_new_with_options(Arc::clone(schema), Vec::new(), &opts)
            .map_err(|e| ProxyError::Conversion(e.to_string()))?;
        return Ok(Some(rb));
    }

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    let mut values_buf: Vec<Option<&Value>> = Vec::with_capacity(num_rows);

    for field in schema.fields() {
        if field.name() == key_column {
            let mut owned_keys: Vec<Option<Value>> = Vec::with_capacity(num_rows);
            for &(k, _) in &found {
                if let Some(uk) = k.user_key() {
                    owned_keys.push(Some(uk.clone()));
                } else {
                    let digest = k.digest();
                    match field.data_type() {
                        DataType::Binary => owned_keys.push(Some(Value::Blob(digest.to_vec()))),
                        DataType::Utf8 => {
                            let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
                            owned_keys.push(Some(Value::String(hex)));
                        }
                        _ => owned_keys.push(None),
                    }
                }
            }
            let refs: Vec<Option<&Value>> = owned_keys.iter().map(|v| v.as_ref()).collect();
            let col = build_array_from_values(
                field.name(),
                field.data_type(),
                &refs,
                type_mismatch_mode,
            )?;
            columns.push(col);
        } else {
            values_buf.clear();
            for &(_, map) in &found {
                values_buf.push(map.get(field.name()));
            }
            let col = build_array_from_values(
                field.name(),
                field.data_type(),
                &values_buf,
                type_mismatch_mode,
            )?;
            columns.push(col);
        }
    }

    let rb = RecordBatch::try_new(Arc::clone(schema), columns)
        .map_err(|e| ProxyError::Conversion(e.to_string()))?;
    Ok(Some(rb))
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
    if join_type == LookupJoinType::Left || selected_indices.len() == num_rows {
        for col in input_batch.columns() {
            out_columns.push(Arc::clone(col));
        }
    } else {
        for col in input_batch.columns() {
            let taken = take_array_by_indices(col.as_ref(), &selected_indices)?;
            out_columns.push(taken);
        }
    }

    // 2. Append requested bin columns (skipping key_column if already present in input_batch)
    let mut values: Vec<Option<&Value>> = Vec::with_capacity(selected_indices.len());
    for bin_field in bins_schema.fields() {
        if bin_field.name() == key_column && input_schema.index_of(key_column).is_ok() {
            continue;
        }
        // Always nullable in Left join mode
        let nullable = bin_field.is_nullable() || join_type == LookupJoinType::Left;
        out_fields.push(Arc::new(bin_field.as_ref().clone().with_nullable(nullable)));

        values.clear();
        for &row_idx in &selected_indices {
            let v = match &outcomes[row_idx] {
                RecordReadOutcome::Found(map) => map.get(bin_field.name()),
                _ => None,
            };
            values.push(v);
        }

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

fn take_array_by_indices(array: &dyn Array, row_indices: &[usize]) -> Result<ArrayRef> {
    if row_indices.is_empty() {
        return Ok(new_empty_array(array.data_type()));
    }
    let data = array.to_data();
    let mut mutable =
        arrow_data::transform::MutableArrayData::new(vec![&data], false, row_indices.len());
    let mut start = row_indices[0];
    let mut end = start + 1;
    for &idx in &row_indices[1..] {
        if idx == end {
            end += 1;
        } else {
            mutable
                .try_extend(0, start, end)
                .map_err(|e| ProxyError::Conversion(e.to_string()))?;
            start = idx;
            end = idx + 1;
        }
    }
    mutable
        .try_extend(0, start, end)
        .map_err(|e| ProxyError::Conversion(e.to_string()))?;
    Ok(make_array(mutable.freeze()))
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
            if *precision == 0 || *precision > 38 {
                return Err(ProxyError::Conversion(format!(
                    "Invalid Decimal128 precision {precision} in column '{col_name}' (must be 1..=38)"
                )));
            }
            let mut b = Decimal128Builder::with_capacity(len)
                .with_data_type(DataType::Decimal128(*precision, *scale));
            for (row, val_opt) in values.iter().enumerate() {
                match val_opt {
                    None | Some(Value::Nil) => b.append_null(),
                    Some(Value::String(s)) => match parse_decimal128(s, *precision, *scale) {
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
                    Some(Value::Int(i)) => match scale_int_to_decimal128(*i, *precision, *scale) {
                        Ok(scaled) => b.append_value(scaled),
                        Err(_) => handle_mismatch(
                            col_name,
                            data_type,
                            &Value::Int(*i),
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
            let mut b =
                TimestampMicrosecondBuilder::with_capacity(len).with_timezone_opt(tz.clone());
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
            let mut b =
                TimestampMillisecondBuilder::with_capacity(len).with_timezone_opt(tz.clone());
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
            let mut b =
                TimestampNanosecondBuilder::with_capacity(len).with_timezone_opt(tz.clone());
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
                        handle_mismatch(
                            col_name,
                            data_type,
                            other,
                            row,
                            type_mismatch_mode,
                            || {
                                nulls.push(false);
                                offsets.push(*offsets.last().unwrap());
                            },
                        )?;
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
                        handle_mismatch(
                            col_name,
                            data_type,
                            other,
                            row,
                            type_mismatch_mode,
                            || {
                                nulls.push(false);
                                offsets.push(*offsets.last().unwrap());
                            },
                        )?;
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
                        handle_mismatch(
                            col_name,
                            data_type,
                            other,
                            row,
                            type_mismatch_mode,
                            || {
                                nulls.push(false);
                                for fv in &mut per_field_vals {
                                    fv.push(None);
                                }
                            },
                        )?;
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

fn validate_decimal128_precision(unscaled: i128, precision: u8) -> std::result::Result<i128, ()> {
    if precision == 0 || precision > 38 {
        return Err(());
    }
    let limit = 10_i128.checked_pow(u32::from(precision)).ok_or(())?;
    if unscaled <= -limit || unscaled >= limit {
        return Err(());
    }
    Ok(unscaled)
}

fn scale_int_to_decimal128(val: i64, precision: u8, scale: i8) -> std::result::Result<i128, ()> {
    let base = i128::from(val);
    let unscaled = if scale >= 0 {
        let factor = 10_i128.checked_pow(scale as u32).ok_or(())?;
        base.checked_mul(factor).ok_or(())?
    } else {
        let divisor = 10_i128.checked_pow((-(scale as i32)) as u32).ok_or(())?;
        base / divisor
    };
    validate_decimal128_precision(unscaled, precision)
}

fn parse_decimal128(s: &str, precision: u8, scale: i8) -> std::result::Result<i128, ()> {
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

    if (int_part.is_empty() && frac_part.is_empty())
        || !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(());
    }

    let unscaled = if scale >= 0 {
        let target_scale = scale as usize;
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
        let normalized = digits.trim_start_matches('0');
        if normalized.is_empty() {
            0_i128
        } else {
            let mag: i128 = normalized.parse().map_err(|_| ())?;
            if neg { -mag } else { mag }
        }
    } else {
        let drop_digits = (-(scale as i32)) as usize;
        if int_part.len() <= drop_digits {
            0_i128
        } else {
            let kept = int_part[..int_part.len() - drop_digits].trim_start_matches('0');
            if kept.is_empty() {
                0_i128
            } else {
                let mag: i128 = kept.parse().map_err(|_| ())?;
                if neg { -mag } else { mag }
            }
        }
    };

    validate_decimal128_precision(unscaled, precision)
}
