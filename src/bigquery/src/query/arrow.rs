// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::error::ConvertError;
#[cfg(google_cloud_unstable_gapic_streaming)]
use crate::error::RowError;
use crate::query::ColumnIndex;
use arrow::array::ArrayRef;
#[cfg(google_cloud_unstable_gapic_streaming)]
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

#[cfg(google_cloud_unstable_gapic_streaming)]
#[derive(Debug)]
pub(crate) struct ArrowStreamDecoder {
    decoder: arrow::ipc::reader::StreamDecoder,
    schema: Option<arrow::datatypes::SchemaRef>,
}

#[cfg(google_cloud_unstable_gapic_streaming)]
impl ArrowStreamDecoder {
    pub(crate) fn new() -> Self {
        Self {
            decoder: arrow::ipc::reader::StreamDecoder::new(),
            schema: None,
        }
    }

    pub(crate) fn set_schema_bytes(&mut self, schema_bytes: &[u8]) -> Result<(), RowError> {
        let mut buf = arrow::buffer::Buffer::from_slice_ref(schema_bytes);
        while let Some(_msg) = self.decoder.decode(&mut buf).map_err(|e| {
            RowError::InvalidRowFormat(format!("failed to decode arrow schema: {e}"))
        })? {}
        self.schema = self.decoder.schema();
        Ok(())
    }

    pub(crate) fn decode_batch(
        &mut self,
        batch_bytes: &[u8],
    ) -> Result<Option<RecordBatch>, RowError> {
        let mut buf = arrow::buffer::Buffer::from_slice_ref(batch_bytes);
        self.decoder
            .decode(&mut buf)
            .map_err(|e| RowError::InvalidRowFormat(format!("failed to decode arrow batch: {e}")))
    }

    pub(crate) fn schema(&self) -> Option<arrow::datatypes::SchemaRef> {
        self.schema.clone()
    }
}

/// A reference to a single cell within an Arrow array.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct ArrowCell {
    array: ArrayRef,
    pub(crate) row_idx: usize,
}

impl PartialEq for ArrowCell {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.array, &other.array) && self.row_idx == other.row_idx
    }
}

impl ArrowCell {
    /// Creates a new `ArrowCell`.
    pub(crate) fn new(array: ArrayRef, row_idx: usize) -> Self {
        Self { array, row_idx }
    }

    /// Returns true if the cell is null.
    pub(crate) fn is_null(&self) -> bool {
        self.array.is_null(self.row_idx)
    }

    fn resolve_index<I: ColumnIndex>(
        &self,
        col: &I,
        struct_arr: &arrow::array::StructArray,
    ) -> Result<usize, ConvertError> {
        col.arrow_index(struct_arr)
            .ok_or_else(|| ConvertError::MissingField(format!("{col}")))
    }

    /// Returns the data type of the underlying array.
    pub(crate) fn data_type(&self) -> &arrow::datatypes::DataType {
        self.array.data_type()
    }

    /// Returns a string representation of the data type.
    pub(crate) fn data_type_str(&self) -> String {
        format!("{:?}", self.array.data_type())
    }

    /// Extracts a child `ArrowCell` from a struct or list array cell by column index or name.
    pub(crate) fn struct_field_cell<I: ColumnIndex>(
        &self,
        index: &I,
    ) -> Result<ArrowCell, ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }

        match self.array.data_type() {
            arrow::datatypes::DataType::Struct(_) => {
                let struct_arr = arrow::array::as_struct_array(&self.array);
                let idx = self.resolve_index(index, struct_arr)?;
                let col = struct_arr.column(idx);
                Ok(ArrowCell {
                    array: col.clone(),
                    row_idx: self.row_idx,
                })
            }
            _ => Err(ConvertError::TypeMismatch {
                expected: "struct array".to_string(),
                got: self.data_type_str(),
            }),
        }
    }

    pub(crate) fn list_element_cell(&self, index: usize) -> Result<ArrowCell, ConvertError> {
        let value_arr = self.list_array_ref()?;
        if index < value_arr.len() {
            Ok(ArrowCell::new(value_arr, index))
        } else {
            Err(ConvertError::MissingField(index.to_string()))
        }
    }

    /// Returns a reference to the nested array for this list row.
    pub(crate) fn list_array_ref(&self) -> Result<arrow::array::ArrayRef, ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }

        match self.array.data_type() {
            arrow::datatypes::DataType::List(_) => {
                let arr = arrow::array::as_list_array(&self.array);
                Ok(arr.value(self.row_idx))
            }
            arrow::datatypes::DataType::LargeList(_) => {
                let arr = arrow::array::as_large_list_array(&self.array);
                Ok(arr.value(self.row_idx))
            }
            _ => Err(ConvertError::TypeMismatch {
                expected: "list array".to_string(),
                got: self.data_type_str(),
            }),
        }
    }

    /// Returns the cell's value as a boolean.
    pub(crate) fn as_bool(&self) -> Result<bool, ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        match self.array.data_type() {
            arrow::datatypes::DataType::Boolean => {
                let arr = arrow::array::as_boolean_array(&self.array);
                Ok(arr.value(self.row_idx))
            }
            _ => Err(ConvertError::TypeMismatch {
                expected: "BooleanArray".to_string(),
                got: self.data_type_str(),
            }),
        }
    }

    /// Returns the cell's value as an `i64`.
    pub(crate) fn as_i64(&self) -> Result<i64, ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        match self.array.data_type() {
            arrow::datatypes::DataType::Int64 => {
                let arr =
                    arrow::array::as_primitive_array::<arrow::datatypes::Int64Type>(&self.array);
                Ok(arr.value(self.row_idx))
            }
            _ => Err(ConvertError::TypeMismatch {
                expected: "Int64Array".to_string(),
                got: self.data_type_str(),
            }),
        }
    }

    /// Returns the cell's value as an `i32`.
    pub(crate) fn as_i32(&self) -> Result<i32, ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        match self.array.data_type() {
            arrow::datatypes::DataType::Int32 => {
                let arr =
                    arrow::array::as_primitive_array::<arrow::datatypes::Int32Type>(&self.array);
                Ok(arr.value(self.row_idx))
            }
            arrow::datatypes::DataType::Int64 => {
                let arr =
                    arrow::array::as_primitive_array::<arrow::datatypes::Int64Type>(&self.array);
                i32::try_from(arr.value(self.row_idx))
                    .map_err(|e| ConvertError::Convert(Box::new(e)))
            }
            _ => Err(ConvertError::TypeMismatch {
                expected: "Int64Array or Int32Array".to_string(),
                got: self.data_type_str(),
            }),
        }
    }

    /// Returns the cell's value as an `f64`.
    pub(crate) fn as_f64(&self) -> Result<f64, ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        match self.array.data_type() {
            arrow::datatypes::DataType::Float64 => {
                let arr =
                    arrow::array::as_primitive_array::<arrow::datatypes::Float64Type>(&self.array);
                Ok(arr.value(self.row_idx))
            }
            _ => Err(ConvertError::TypeMismatch {
                expected: "Float64Array".to_string(),
                got: self.data_type_str(),
            }),
        }
    }

    /// Returns the cell's value as an `f32`.
    pub(crate) fn as_f32(&self) -> Result<f32, ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        match self.array.data_type() {
            arrow::datatypes::DataType::Float32 => {
                let arr =
                    arrow::array::as_primitive_array::<arrow::datatypes::Float32Type>(&self.array);
                Ok(arr.value(self.row_idx))
            }
            arrow::datatypes::DataType::Float64 => {
                let arr =
                    arrow::array::as_primitive_array::<arrow::datatypes::Float64Type>(&self.array);
                Ok(arr.value(self.row_idx) as f32)
            }
            _ => Err(ConvertError::TypeMismatch {
                expected: "Float64Array or Float32Array".to_string(),
                got: self.data_type_str(),
            }),
        }
    }

    /// Returns the cell's value as a string slice (`&str`).
    pub(crate) fn as_str(&self) -> Result<&str, ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        match self.array.data_type() {
            arrow::datatypes::DataType::Utf8 => {
                let arr = arrow::array::as_string_array(&self.array);
                Ok(arr.value(self.row_idx))
            }
            arrow::datatypes::DataType::LargeUtf8 => {
                let arr = arrow::array::as_largestring_array(&self.array);
                Ok(arr.value(self.row_idx))
            }
            _ => Err(ConvertError::TypeMismatch {
                expected: "StringArray or LargeStringArray".to_string(),
                got: self.data_type_str(),
            }),
        }
    }

    /// Returns the cell's value as a byte slice (`&[u8]`).
    pub(crate) fn as_bytes(&self) -> Result<&[u8], ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        match self.array.data_type() {
            arrow::datatypes::DataType::Binary => {
                let arr = arrow::array::as_generic_binary_array::<i32>(&self.array);
                Ok(arr.value(self.row_idx))
            }
            arrow::datatypes::DataType::LargeBinary => {
                let arr = arrow::array::as_generic_binary_array::<i64>(&self.array);
                Ok(arr.value(self.row_idx))
            }
            _ => Err(ConvertError::TypeMismatch {
                expected: "BinaryArray or LargeBinaryArray".to_string(),
                got: self.data_type_str(),
            }),
        }
    }

    /// Returns timestamp value in microseconds since Unix epoch.
    pub(crate) fn as_timestamp_micros(&self) -> Result<i64, ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        match self.array.data_type() {
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => {
                let arr = arrow::array::as_primitive_array::<
                    arrow::datatypes::TimestampMicrosecondType,
                >(&self.array);
                Ok(arr.value(self.row_idx))
            }
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, _) => {
                let arr = arrow::array::as_primitive_array::<
                    arrow::datatypes::TimestampMillisecondType,
                >(&self.array);
                Ok(arr.value(self.row_idx) * 1_000)
            }
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
                let arr = arrow::array::as_primitive_array::<
                    arrow::datatypes::TimestampNanosecondType,
                >(&self.array);
                Ok(arr.value(self.row_idx) / 1_000)
            }
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Second, _) => {
                let arr = arrow::array::as_primitive_array::<arrow::datatypes::TimestampSecondType>(
                    &self.array,
                );
                Ok(arr.value(self.row_idx) * 1_000_000)
            }
            _ => Err(ConvertError::TypeMismatch {
                expected: "TimestampArray".to_string(),
                got: self.data_type_str(),
            }),
        }
    }

    /// Returns date value in days since Unix epoch.
    pub(crate) fn as_date32(&self) -> Result<i32, ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        match self.array.data_type() {
            arrow::datatypes::DataType::Date32 => {
                let arr =
                    arrow::array::as_primitive_array::<arrow::datatypes::Date32Type>(&self.array);
                Ok(arr.value(self.row_idx))
            }
            _ => Err(ConvertError::TypeMismatch {
                expected: "Date32Array".to_string(),
                got: self.data_type_str(),
            }),
        }
    }

    /// Returns time value in microseconds since midnight.
    pub(crate) fn as_time64_micros(&self) -> Result<i64, ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        match self.array.data_type() {
            arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond) => {
                let arr = arrow::array::as_primitive_array::<arrow::datatypes::Time64MicrosecondType>(
                    &self.array,
                );
                Ok(arr.value(self.row_idx))
            }
            arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Nanosecond) => {
                let arr = arrow::array::as_primitive_array::<arrow::datatypes::Time64NanosecondType>(
                    &self.array,
                );
                Ok(arr.value(self.row_idx) / 1_000)
            }
            _ => Err(ConvertError::TypeMismatch {
                expected: "Time64MicrosecondArray".to_string(),
                got: self.data_type_str(),
            }),
        }
    }

    /// Returns interval month-day-nano value.
    pub(crate) fn as_interval(
        &self,
    ) -> Result<arrow::datatypes::IntervalMonthDayNano, ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        match self.array.data_type() {
            arrow::datatypes::DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano) => {
                let arr = arrow::array::as_primitive_array::<
                    arrow::datatypes::IntervalMonthDayNanoType,
                >(&self.array);
                Ok(arr.value(self.row_idx))
            }
            _ => Err(ConvertError::TypeMismatch {
                expected: "IntervalMonthDayNanoArray".to_string(),
                got: self.data_type_str(),
            }),
        }
    }

    /// Returns decimal value as a formatted string.
    pub(crate) fn as_decimal_str(&self) -> Result<String, ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        match self.array.data_type() {
            arrow::datatypes::DataType::Decimal128(_, _) => {
                let arr = arrow::array::as_primitive_array::<arrow::datatypes::Decimal128Type>(
                    &self.array,
                );
                Ok(arr.value_as_string(self.row_idx))
            }
            arrow::datatypes::DataType::Decimal256(_, _) => {
                let arr = arrow::array::as_primitive_array::<arrow::datatypes::Decimal256Type>(
                    &self.array,
                );
                Ok(arr.value_as_string(self.row_idx))
            }
            _ => Err(ConvertError::TypeMismatch {
                expected: "Decimal128Array or Decimal256Array".to_string(),
                got: self.data_type_str(),
            }),
        }
    }

    /// Returns decimal128 value along with scale.
    pub(crate) fn as_decimal128_with_scale(&self) -> Result<(i128, u32), ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        match self.array.data_type() {
            arrow::datatypes::DataType::Decimal128(_, scale) => {
                let arr = arrow::array::as_primitive_array::<arrow::datatypes::Decimal128Type>(
                    &self.array,
                );
                Ok((arr.value(self.row_idx), *scale as u32))
            }
            _ => Err(ConvertError::TypeMismatch {
                expected: "Decimal128Array".to_string(),
                got: self.data_type_str(),
            }),
        }
    }
}
