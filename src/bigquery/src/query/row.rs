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

use crate::error::{ConvertError, RowError};
use crate::query::Schema;
use crate::query::from_sql::FromSql;
use std::sync::Arc;

pub type Result<T> = std::result::Result<T, RowError>;

/// A row in a query result.
#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    pub(crate) values: Vec<wkt::Value>,
    pub(crate) schema: Arc<Schema>,
}

pub(crate) mod private {
    /// A sealed trait to prevent external implementation of `ColumnIndex`.
    pub trait Sealed {}
    impl Sealed for usize {}
    impl Sealed for &str {}
    impl Sealed for String {}
}

/// A trait for types that can be used to index into a [`Row`].
///
/// This trait is sealed and cannot be implemented for types outside of this crate.
pub trait ColumnIndex: private::Sealed + std::fmt::Debug {
    /// Returns the index of the column in the given row, if it exists.
    fn index(&self, row: &Row) -> Option<usize>;
}

impl ColumnIndex for usize {
    fn index(&self, row: &Row) -> Option<usize> {
        if *self < row.values.len() {
            Some(*self)
        } else {
            None
        }
    }
}

impl ColumnIndex for &str {
    fn index(&self, row: &Row) -> Option<usize> {
        row.schema
            .fields
            .iter()
            .position(|field| field.name == *self)
    }
}

impl ColumnIndex for String {
    fn index(&self, row: &Row) -> Option<usize> {
        self.as_str().index(row)
    }
}

impl Row {
    /// Returns the raw values of the row.
    pub fn raw_values(&self) -> &[wkt::Value] {
        &self.values
    }

    /// Retrieves a value from the row by column name or zero-based index.
    pub fn try_get<T: FromSql, I: ColumnIndex>(&self, index: I) -> Result<T> {
        let idx = index
            .index(self)
            .ok_or_else(|| RowError::ColumnNotFound(format!("{:?}", index)))?;
        let val = self
            .values
            .get(idx)
            .ok_or_else(|| RowError::IndexOutOfRange {
                index: idx,
                len: self.values.len(),
            })?;

        T::from_sql(val.clone()).map_err(|e| RowError::TypeConversion {
            column: format!("{:?}", index),
            source: e,
        })
    }

    /// Retrieves a value from the row by column name or zero-based index, panicking on error.
    pub fn get<T: FromSql, I: ColumnIndex>(&self, index: I) -> T {
        self.try_get(index).unwrap()
    }
}

fn get_cells_from_struct(mut obj: wkt::Struct) -> Result<Vec<wkt::Value>> {
    let f_val = obj
        .remove("f")
        .ok_or_else(|| RowError::InvalidRowFormat("missing nested cell array 'f'".into()))?;
    match f_val {
        wkt::Value::Array(arr) => Ok(arr),
        _ => Err(RowError::InvalidRowFormat(
            "nested cell array 'f' is not an array".into(),
        )),
    }
}

fn extract_value_from_cell(cell: wkt::Value) -> Result<wkt::Value> {
    match cell {
        wkt::Value::Object(mut obj) => {
            let v_val = obj.remove("v").unwrap_or(wkt::Value::Null);
            Ok(v_val)
        }
        wkt::Value::Null => Ok(wkt::Value::Null),
        _ => Err(RowError::InvalidRowFormat(
            "cell value is not an object".into(),
        )),
    }
}

fn normalize_value(
    val: wkt::Value,
    field_type: &str,
    fields: &[google_cloud_bigquery_v2::model::TableFieldSchema],
) -> Result<wkt::Value> {
    match val {
        wkt::Value::Null => Ok(wkt::Value::Null),
        wkt::Value::String(s) => match field_type {
            "INTEGER" | "INT64" => {
                let num = s.parse::<i64>().map_err(|e| RowError::TypeConversion {
                    column: "unknown".to_string(),
                    source: ConvertError::Convert(Box::new(e)),
                })?;
                Ok(wkt::Value::Number(serde_json::Number::from(num)))
            }
            "FLOAT" | "FLOAT64" => {
                let num = s.parse::<f64>().map_err(|e| RowError::TypeConversion {
                    column: "unknown".to_string(),
                    source: ConvertError::Convert(Box::new(e)),
                })?;
                Ok(wkt::Value::Number(
                    serde_json::Number::from_f64(num)
                        .unwrap_or_else(|| serde_json::Number::from(0)),
                ))
            }
            "BOOLEAN" | "BOOL" => {
                let b = s.parse::<bool>().map_err(|e| RowError::TypeConversion {
                    column: "unknown".to_string(),
                    source: ConvertError::Convert(Box::new(e)),
                })?;
                Ok(wkt::Value::Bool(b))
            }
            _ => Ok(wkt::Value::String(s)),
        },
        wkt::Value::Object(obj) => {
            let cells = get_cells_from_struct(obj)?;
            let mut nested_obj = wkt::Struct::new();
            for (i, cell) in cells.into_iter().enumerate() {
                if let Some(sub_field) = fields.get(i) {
                    let cell_val = extract_value_from_cell(cell)?;
                    let sub_fields = &sub_field.fields;
                    let norm_val = normalize_value(cell_val, &sub_field.r#type, sub_fields)?;
                    nested_obj.insert(sub_field.name.clone(), norm_val);
                }
            }
            Ok(wkt::Value::Object(nested_obj))
        }
        wkt::Value::Array(arr) => {
            let mut normalized_arr = Vec::with_capacity(arr.len());
            for cell in arr {
                let cell_val = extract_value_from_cell(cell)?;
                let norm_val = normalize_value(cell_val, field_type, fields)?;
                normalized_arr.push(norm_val);
            }
            Ok(wkt::Value::Array(normalized_arr))
        }
        other => Ok(other),
    }
}

pub(crate) fn convert_row(raw_row: wkt::Struct, schema: &Arc<Schema>) -> Result<Row> {
    let cells = get_cells_from_struct(raw_row)?;

    if cells.len() != schema.fields.len() {
        return Err(RowError::InvalidRowFormat(format!(
            "schema and row cell mismatch (expected {}, got {})",
            schema.fields.len(),
            cells.len()
        )));
    }

    let mut values = Vec::with_capacity(cells.len());
    for (i, cell) in cells.into_iter().enumerate() {
        let raw_val = extract_value_from_cell(cell)?;
        let field_def = &schema.fields[i];
        let fields = &field_def.fields;
        let normalized_val = normalize_value(raw_val, &field_def.r#type, fields)?;
        values.push(normalized_val);
    }

    Ok(Row {
        values,
        schema: Arc::clone(schema),
    })
}
