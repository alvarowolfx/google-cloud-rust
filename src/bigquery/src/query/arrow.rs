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

use crate::error::RowError;
use crate::query::{Row, Schema};
use arrow::array::{
    Array, BooleanArray, Date32Array, Date64Array, Decimal128Array, Decimal256Array, Float32Array,
    Float64Array, GenericBinaryArray, GenericListArray, GenericStringArray, Int8Array, Int16Array,
    Int32Array, Int64Array, StructArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use arrow::record_batch::RecordBatch;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use std::sync::Arc;
use wkt::{ListValue, Struct, Value};

/// Decodes serialized Arrow schema and batch bytes into an Arrow [`RecordBatch`].
pub(crate) fn decode_arrow_batch(
    serialized_schema: &[u8],
    serialized_batch: &[u8],
) -> Result<RecordBatch, RowError> {
    let mut buf = Vec::with_capacity(serialized_schema.len() + serialized_batch.len());
    buf.extend_from_slice(serialized_schema);
    buf.extend_from_slice(serialized_batch);
    let cursor = std::io::Cursor::new(buf);
    let mut reader = arrow::ipc::reader::StreamReader::try_new(cursor, None)
        .map_err(|e| RowError::InvalidRowFormat(format!("failed to parse arrow stream: {e}")))?;
    let batch = reader
        .next()
        .transpose()
        .map_err(|e| RowError::InvalidRowFormat(format!("failed to read arrow record batch: {e}")))?
        .ok_or_else(|| RowError::InvalidRowFormat("empty arrow record batch".into()))?;
    Ok(batch)
}

/// Converts an Arrow [`RecordBatch`] into a vector of [`Row`]s using the table schema.
pub(crate) fn record_batch_to_rows(
    batch: &RecordBatch,
    schema: &Arc<Schema>,
) -> Result<Vec<Row>, RowError> {
    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();
    let mut rows = Vec::with_capacity(num_rows);

    for row_idx in 0..num_rows {
        let mut row_values = ListValue::new();
        for col_idx in 0..num_cols {
            let col = batch.column(col_idx);
            let val = arrow_cell_to_wkt_value(col.as_ref(), row_idx)?;
            row_values.push(val);
        }
        rows.push(Row {
            values: Value::Array(row_values),
            schema: schema.clone(),
        });
    }

    Ok(rows)
}

/// Converts a single cell from an Arrow array at index `row_idx` into a [`wkt::Value`].
pub(crate) fn arrow_cell_to_wkt_value(
    array: &dyn Array,
    row_idx: usize,
) -> Result<Value, RowError> {
    if array.is_null(row_idx) {
        return Ok(Value::Null);
    }

    match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(Value::Bool(arr.value(row_idx)))
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            Ok(Value::Number(serde_json::Number::from(arr.value(row_idx))))
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            Ok(Value::Number(serde_json::Number::from(arr.value(row_idx))))
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(Value::Number(serde_json::Number::from(arr.value(row_idx))))
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(Value::Number(serde_json::Number::from(arr.value(row_idx))))
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            Ok(Value::Number(serde_json::Number::from(arr.value(row_idx))))
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            Ok(Value::Number(serde_json::Number::from(arr.value(row_idx))))
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            Ok(Value::Number(serde_json::Number::from(arr.value(row_idx))))
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            Ok(Value::Number(serde_json::Number::from(arr.value(row_idx))))
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            let v = arr.value(row_idx);
            if v.is_nan() {
                Ok(Value::String("NaN".to_string()))
            } else if v == f32::INFINITY {
                Ok(Value::String("+inf".to_string()))
            } else if v == f32::NEG_INFINITY {
                Ok(Value::String("-inf".to_string()))
            } else {
                serde_json::Number::from_f64(v as f64)
                    .map(Value::Number)
                    .ok_or_else(|| RowError::InvalidRowFormat("invalid float32".into()))
            }
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            let v = arr.value(row_idx);
            if v.is_nan() {
                Ok(Value::String("NaN".to_string()))
            } else if v == f64::INFINITY {
                Ok(Value::String("+inf".to_string()))
            } else if v == f64::NEG_INFINITY {
                Ok(Value::String("-inf".to_string()))
            } else {
                serde_json::Number::from_f64(v)
                    .map(Value::Number)
                    .ok_or_else(|| RowError::InvalidRowFormat("invalid float64".into()))
            }
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<GenericStringArray<i32>>()
                .unwrap();
            Ok(Value::String(arr.value(row_idx).to_string()))
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<GenericStringArray<i64>>()
                .unwrap();
            Ok(Value::String(arr.value(row_idx).to_string()))
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<GenericBinaryArray<i32>>()
                .unwrap();
            Ok(Value::String(BASE64_STANDARD.encode(arr.value(row_idx))))
        }
        DataType::LargeBinary => {
            let arr = array
                .as_any()
                .downcast_ref::<GenericBinaryArray<i64>>()
                .unwrap();
            Ok(Value::String(BASE64_STANDARD.encode(arr.value(row_idx))))
        }
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let days = arr.value(row_idx);
            let epoch = time::Date::from_calendar_date(1970, time::Month::January, 1)
                .map_err(|e| RowError::InvalidRowFormat(e.to_string()))?;
            let date = epoch + time::Duration::days(days as i64);
            let formatted = date
                .format(crate::query::from_sql::BIGQUERY_DATE_FORMAT)
                .map_err(|e| RowError::InvalidRowFormat(e.to_string()))?;
            Ok(Value::String(formatted))
        }
        DataType::Date64 => {
            let arr = array.as_any().downcast_ref::<Date64Array>().unwrap();
            let millis = arr.value(row_idx);
            let days = millis / 86_400_000;
            let epoch = time::Date::from_calendar_date(1970, time::Month::January, 1)
                .map_err(|e| RowError::InvalidRowFormat(e.to_string()))?;
            let date = epoch + time::Duration::days(days);
            let formatted = date
                .format(crate::query::from_sql::BIGQUERY_DATE_FORMAT)
                .map_err(|e| RowError::InvalidRowFormat(e.to_string()))?;
            Ok(Value::String(formatted))
        }
        DataType::Time32(unit) => match unit {
            TimeUnit::Second => {
                let arr = array.as_any().downcast_ref::<Time32SecondArray>().unwrap();
                let secs = arr.value(row_idx);
                let time = time::Time::from_hms(
                    (secs / 3600) as u8,
                    ((secs % 3600) / 60) as u8,
                    (secs % 60) as u8,
                )
                .map_err(|e| RowError::InvalidRowFormat(e.to_string()))?;
                let formatted = time
                    .format(crate::query::from_sql::BIGQUERY_TIME_FORMAT)
                    .map_err(|e| RowError::InvalidRowFormat(e.to_string()))?;
                Ok(Value::String(formatted))
            }
            TimeUnit::Millisecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Time32MillisecondArray>()
                    .unwrap();
                let millis = arr.value(row_idx);
                let time = time::Time::from_hms_milli(
                    (millis / 3_600_000) as u8,
                    ((millis % 3_600_000) / 60_000) as u8,
                    ((millis % 60_000) / 1000) as u8,
                    (millis % 1000) as u16,
                )
                .map_err(|e| RowError::InvalidRowFormat(e.to_string()))?;
                let formatted = time
                    .format(crate::query::from_sql::BIGQUERY_TIME_SUBSEC_FORMAT)
                    .map_err(|e| RowError::InvalidRowFormat(e.to_string()))?;
                Ok(Value::String(formatted))
            }
            _ => Err(RowError::InvalidRowFormat(format!(
                "unsupported time32 unit: {unit:?}"
            ))),
        },
        DataType::Time64(unit) => match unit {
            TimeUnit::Microsecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Time64MicrosecondArray>()
                    .unwrap();
                let micros = arr.value(row_idx);
                let time = time::Time::from_hms_micro(
                    (micros / 3_600_000_000) as u8,
                    ((micros % 3_600_000_000) / 60_000_000) as u8,
                    ((micros % 60_000_000) / 1_000_000) as u8,
                    (micros % 1_000_000) as u32,
                )
                .map_err(|e| RowError::InvalidRowFormat(e.to_string()))?;
                let formatted = time
                    .format(crate::query::from_sql::BIGQUERY_TIME_SUBSEC_FORMAT)
                    .map_err(|e| RowError::InvalidRowFormat(e.to_string()))?;
                Ok(Value::String(formatted))
            }
            TimeUnit::Nanosecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Time64NanosecondArray>()
                    .unwrap();
                let nanos = arr.value(row_idx);
                let time = time::Time::from_hms_nano(
                    (nanos / 3_600_000_000_000) as u8,
                    ((nanos % 3_600_000_000_000) / 60_000_000_000) as u8,
                    ((nanos % 60_000_000_000) / 1_000_000_000) as u8,
                    (nanos % 1_000_000_000) as u32,
                )
                .map_err(|e| RowError::InvalidRowFormat(e.to_string()))?;
                let formatted = time
                    .format(crate::query::from_sql::BIGQUERY_TIME_SUBSEC_FORMAT)
                    .map_err(|e| RowError::InvalidRowFormat(e.to_string()))?;
                Ok(Value::String(formatted))
            }
            _ => Err(RowError::InvalidRowFormat(format!(
                "unsupported time64 unit: {unit:?}"
            ))),
        },
        DataType::Timestamp(unit, _) => {
            let micros = match unit {
                TimeUnit::Second => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .unwrap();
                    arr.value(row_idx) * 1_000_000
                }
                TimeUnit::Millisecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap();
                    arr.value(row_idx) * 1_000
                }
                TimeUnit::Microsecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    arr.value(row_idx)
                }
                TimeUnit::Nanosecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap();
                    arr.value(row_idx) / 1_000
                }
            };
            Ok(Value::Number(serde_json::Number::from(micros)))
        }
        DataType::Decimal128(_, _) => {
            let arr = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            Ok(Value::String(arr.value_as_string(row_idx)))
        }
        DataType::Decimal256(_, _) => {
            let arr = array.as_any().downcast_ref::<Decimal256Array>().unwrap();
            Ok(Value::String(arr.value_as_string(row_idx)))
        }
        DataType::List(_) => {
            let arr = array
                .as_any()
                .downcast_ref::<GenericListArray<i32>>()
                .unwrap();
            let sub_arr = arr.value(row_idx);
            let mut list = ListValue::new();
            for i in 0..sub_arr.len() {
                list.push(arrow_cell_to_wkt_value(sub_arr.as_ref(), i)?);
            }
            Ok(Value::Array(list))
        }
        DataType::LargeList(_) => {
            let arr = array
                .as_any()
                .downcast_ref::<GenericListArray<i64>>()
                .unwrap();
            let sub_arr = arr.value(row_idx);
            let mut list = ListValue::new();
            for i in 0..sub_arr.len() {
                list.push(arrow_cell_to_wkt_value(sub_arr.as_ref(), i)?);
            }
            Ok(Value::Array(list))
        }
        DataType::Struct(fields) => {
            let arr = array.as_any().downcast_ref::<StructArray>().unwrap();
            let mut obj = Struct::new();
            for (col_idx, field) in fields.iter().enumerate() {
                let sub_col = arr.column(col_idx);
                let val = arrow_cell_to_wkt_value(sub_col.as_ref(), row_idx)?;
                obj.insert(field.name().clone(), val);
            }
            Ok(Value::Object(obj))
        }
        DataType::Interval(unit) => match unit {
            IntervalUnit::MonthDayNano => Ok(Value::String(format!("{:?}", array))),
            _ => Err(RowError::InvalidRowFormat(format!(
                "unsupported interval unit: {unit:?}"
            ))),
        },
        other => Err(RowError::InvalidRowFormat(format!(
            "unsupported Arrow datatype: {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array,
        ListBuilder, StringBuilder, StructArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    };
    use arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema, TimeUnit};
    use arrow::ipc::writer::StreamWriter;
    use google_cloud_bigquery_v2::model::{TableFieldSchema, TableSchema};
    use std::sync::Arc;

    #[test]
    fn test_arrow_batch_decode_and_row_conversion() -> anyhow::Result<()> {
        let fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("active", DataType::Boolean, false),
            Field::new("score", DataType::Float64, false),
            Field::new("created_date", DataType::Date32, false),
            Field::new(
                "created_time",
                DataType::Time64(TimeUnit::Microsecond),
                false,
            ),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("amount", DataType::Decimal128(10, 2), false),
            Field::new("payload", DataType::Binary, false),
            Field::new("opt_str", DataType::Utf8, true),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                false,
            ),
        ];

        let schema = Arc::new(ArrowSchema::new(fields));

        let id_arr = Int64Array::from(vec![42, 100]);
        let name_arr = GenericStringArray::<i32>::from(vec!["Alice", "Bob"]);
        let active_arr = BooleanArray::from(vec![true, false]);
        let score_arr = Float64Array::from(vec![99.5, 85.0]);
        // 1970-01-01 + 19000 days = ~2022
        let date_arr = Date32Array::from(vec![19000, 19001]);
        // 15:30:00 = 15*3600 + 30*60 = 55800 seconds = 55_800_000_000 microseconds
        let time_arr = Time64MicrosecondArray::from(vec![55_800_000_000, 36_000_000_000]);
        // 1600000000000000 microseconds
        let ts_arr =
            TimestampMicrosecondArray::from(vec![1_600_000_000_000_000, 1_700_000_000_000_000]);
        let dec_arr = Decimal128Array::from_iter_values(vec![12345, 67890])
            .with_precision_and_scale(10, 2)?;
        let binary_arr = BinaryArray::from(vec![&b"hello"[..], &b"world"[..]]);
        let opt_str_arr = GenericStringArray::<i32>::from(vec![Some("present"), None]);

        let mut list_builder = ListBuilder::new(StringBuilder::new());
        list_builder.values().append_value("tag1");
        list_builder.values().append_value("tag2");
        list_builder.append(true);
        list_builder.values().append_value("tag3");
        list_builder.append(true);
        let tags_arr = list_builder.finish();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_arr),
                Arc::new(name_arr),
                Arc::new(active_arr),
                Arc::new(score_arr),
                Arc::new(date_arr),
                Arc::new(time_arr),
                Arc::new(ts_arr),
                Arc::new(dec_arr),
                Arc::new(binary_arr),
                Arc::new(opt_str_arr),
                Arc::new(tags_arr),
            ],
        )?;

        // Serialize schema & batch
        let mut schema_buf = Vec::new();
        {
            let _ = StreamWriter::try_new(&mut schema_buf, &schema)?;
        }
        let schema_len = schema_buf.len();

        let mut full_buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut full_buf, &schema)?;
            writer.write(&batch)?;
            writer.finish()?;
        }
        let batch_buf = full_buf[schema_len..].to_vec();

        // Test decode_arrow_batch
        let decoded_batch = decode_arrow_batch(&schema_buf, &batch_buf)?;
        assert_eq!(decoded_batch.num_rows(), 2);
        assert_eq!(decoded_batch.num_columns(), 11);

        // Build BQ TableSchema
        let bq_schema = Arc::new(Schema::new(TableSchema::new().set_fields(vec![
            TableFieldSchema::new().set_name("id").set_type("INTEGER"),
            TableFieldSchema::new().set_name("name").set_type("STRING"),
            TableFieldSchema::new().set_name("active").set_type("BOOLEAN"),
            TableFieldSchema::new().set_name("score").set_type("FLOAT"),
            TableFieldSchema::new().set_name("created_date").set_type("DATE"),
            TableFieldSchema::new().set_name("created_time").set_type("TIME"),
            TableFieldSchema::new().set_name("created_at").set_type("TIMESTAMP"),
            TableFieldSchema::new().set_name("amount").set_type("NUMERIC"),
            TableFieldSchema::new().set_name("payload").set_type("BYTES"),
            TableFieldSchema::new().set_name("opt_str").set_type("STRING"),
            TableFieldSchema::new()
                .set_name("tags")
                .set_type("STRING")
                .set_mode("REPEATED"),
        ])));

        let rows = record_batch_to_rows(&decoded_batch, &bq_schema)?;
        assert_eq!(rows.len(), 2);

        // Verify Row 0
        let row0 = &rows[0];
        assert_eq!(row0.get::<i64, _>("id"), 42);
        assert_eq!(row0.get::<String, _>("name"), "Alice");
        assert!(row0.get::<bool, _>("active"));
        assert_eq!(row0.get::<f64, _>("score"), 99.5);
        let date: google_cloud_type::model::Date = row0.get("created_date");
        assert_eq!(date.year, 2022);
        let time: google_cloud_type::model::TimeOfDay = row0.get("created_time");
        assert_eq!(time.hours, 15);
        assert_eq!(time.minutes, 30);
        let ts: wkt::Timestamp = row0.get("created_at");
        assert_eq!(ts.seconds(), 1_600_000_000);
        let dec: rust_decimal::Decimal = row0.get("amount");
        assert_eq!(dec.to_string(), "123.45");
        let bytes: Vec<u8> = row0.get("payload");
        assert_eq!(bytes, b"hello");
        let opt: Option<String> = row0.get("opt_str");
        assert_eq!(opt, Some("present".to_string()));
        let tags: Vec<String> = row0.get("tags");
        assert_eq!(tags, vec!["tag1", "tag2"]);

        // Verify Row 1 (Null option)
        let row1 = &rows[1];
        assert_eq!(row1.get::<i64, _>("id"), 100);
        assert_eq!(row1.get::<String, _>("name"), "Bob");
        let opt1: Option<String> = row1.get("opt_str");
        assert_eq!(opt1, None);
        let tags1: Vec<String> = row1.get("tags");
        assert_eq!(tags1, vec!["tag3"]);

        Ok(())
    }

    #[test]
    fn test_struct_and_nested_conversion() -> anyhow::Result<()> {
        let inner_fields = Fields::from(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("zip", DataType::Int64, false),
        ]);
        let struct_field = Field::new("address", DataType::Struct(inner_fields.clone()), false);
        let schema = Arc::new(ArrowSchema::new(vec![struct_field]));

        let city_arr = GenericStringArray::<i32>::from(vec!["New York"]);
        let zip_arr = Int64Array::from(vec![10001]);
        let struct_arr = StructArray::new(
            inner_fields,
            vec![Arc::new(city_arr), Arc::new(zip_arr)],
            None,
        );

        let batch = RecordBatch::try_new(schema, vec![Arc::new(struct_arr)])?;

        let bq_schema = Arc::new(Schema::new(TableSchema::new().set_fields(vec![
            TableFieldSchema::new().set_name("address").set_type("RECORD").set_fields(vec![
                TableFieldSchema::new().set_name("city").set_type("STRING"),
                TableFieldSchema::new().set_name("zip").set_type("INTEGER"),
            ]),
        ])));

        let rows = record_batch_to_rows(&batch, &bq_schema)?;
        assert_eq!(rows.len(), 1);
        let obj: wkt::Struct = rows[0].get("address");
        assert_eq!(obj.get("city").and_then(|v| v.as_str()), Some("New York"));
        assert_eq!(obj.get("zip").and_then(|v| v.as_i64()), Some(10001));

        Ok(())
    }
}
