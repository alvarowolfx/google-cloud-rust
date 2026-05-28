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

use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use serde::Deserialize;
use std::str::FromStr;

type BoxedError = Box<dyn std::error::Error + Send + Sync>;

/// Represent failures in converting a BigQuery Value to a Rust type.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum ConversionError {
    /// The value kind is not what we expected.
    #[error("type mismatch, expected {expected}, got {got:?}")]
    TypeMismatch {
        expected: &'static str,
        got: wkt::Value,
    },

    /// The value is null, but the target type does not support nulls.
    #[error("expected non-null value, got null")]
    NotNull,

    /// There was a problem during conversion.
    #[error("cannot convert value, source={0}")]
    Convert(#[source] BoxedError),
}

/// Converts BigQuery [wkt::Value] to Rust types.
pub trait FromSql: Sized {
    fn from_sql(value: wkt::Value) -> Result<Self, ConversionError>;
}

impl FromSql for wkt::Value {
    fn from_sql(value: wkt::Value) -> Result<Self, ConversionError> {
        Ok(value)
    }
}

impl FromSql for String {
    fn from_sql(value: wkt::Value) -> Result<Self, ConversionError> {
        match value {
            wkt::Value::String(s) => Ok(s),
            wkt::Value::Null => Err(ConversionError::NotNull),
            other => Err(ConversionError::TypeMismatch {
                expected: "string",
                got: other,
            }),
        }
    }
}

impl FromSql for i64 {
    fn from_sql(value: wkt::Value) -> Result<Self, ConversionError> {
        match value {
            wkt::Value::Number(n) => n.as_i64().ok_or_else(|| {
                ConversionError::Convert("number is not a valid i64".into())
            }),
            wkt::Value::String(s) => s.parse::<i64>().map_err(|e| {
                ConversionError::Convert(Box::new(e))
            }),
            wkt::Value::Null => Err(ConversionError::NotNull),
            other => Err(ConversionError::TypeMismatch {
                expected: "number or string",
                got: other,
            }),
        }
    }
}

impl FromSql for i32 {
    fn from_sql(value: wkt::Value) -> Result<Self, ConversionError> {
        match value {
            wkt::Value::Number(n) => n.as_i64().map(|v| v as i32).ok_or_else(|| {
                ConversionError::Convert("number is not a valid i32".into())
            }),
            wkt::Value::String(s) => s.parse::<i32>().map_err(|e| {
                ConversionError::Convert(Box::new(e))
            }),
            wkt::Value::Null => Err(ConversionError::NotNull),
            other => Err(ConversionError::TypeMismatch {
                expected: "number or string",
                got: other,
            }),
        }
    }
}

impl FromSql for f64 {
    fn from_sql(value: wkt::Value) -> Result<Self, ConversionError> {
        match value {
            wkt::Value::Number(n) => n.as_f64().ok_or_else(|| {
                ConversionError::Convert("invalid f64 number".into())
            }),
            wkt::Value::String(s) => s.parse::<f64>().map_err(|e| {
                ConversionError::Convert(Box::new(e))
            }),
            wkt::Value::Null => Err(ConversionError::NotNull),
            other => Err(ConversionError::TypeMismatch {
                expected: "number or string",
                got: other,
            }),
        }
    }
}

impl FromSql for f32 {
    fn from_sql(value: wkt::Value) -> Result<Self, ConversionError> {
        match value {
            wkt::Value::Number(n) => n.as_f64().map(|v| v as f32).ok_or_else(|| {
                ConversionError::Convert("invalid f32 number".into())
            }),
            wkt::Value::String(s) => s.parse::<f32>().map_err(|e| {
                ConversionError::Convert(Box::new(e))
            }),
            wkt::Value::Null => Err(ConversionError::NotNull),
            other => Err(ConversionError::TypeMismatch {
                expected: "number or string",
                got: other,
            }),
        }
    }
}

impl FromSql for bool {
    fn from_sql(value: wkt::Value) -> Result<Self, ConversionError> {
        match value {
            wkt::Value::Bool(b) => Ok(b),
            wkt::Value::String(s) => s.parse::<bool>().map_err(|e| {
                ConversionError::Convert(Box::new(e))
            }),
            wkt::Value::Null => Err(ConversionError::NotNull),
            other => Err(ConversionError::TypeMismatch {
                expected: "bool or string",
                got: other,
            }),
        }
    }
}

impl FromSql for Vec<u8> {
    fn from_sql(value: wkt::Value) -> Result<Self, ConversionError> {
        match value {
            wkt::Value::String(s) => BASE64_STANDARD
                .decode(s)
                .map_err(|e| ConversionError::Convert(Box::new(e))),
            wkt::Value::Null => Err(ConversionError::NotNull),
            other => Err(ConversionError::TypeMismatch {
                expected: "string (base64 encoded)",
                got: other,
            }),
        }
    }
}

impl<T: FromSql> FromSql for Option<T> {
    fn from_sql(value: wkt::Value) -> Result<Self, ConversionError> {
        match value {
            wkt::Value::Null => Ok(None),
            other => T::from_sql(other).map(Some),
        }
    }
}

impl<T: FromSql> FromSql for Vec<T> {
    fn from_sql(value: wkt::Value) -> Result<Self, ConversionError> {
        match value {
            wkt::Value::Array(arr) => arr.into_iter().map(T::from_sql).collect(),
            wkt::Value::Null => Err(ConversionError::NotNull),
            other => Err(ConversionError::TypeMismatch {
                expected: "array",
                got: other,
            }),
        }
    }
}



impl FromSql for google_cloud_type::model::Decimal {
    fn from_sql(value: wkt::Value) -> Result<Self, ConversionError> {
        match value {
            wkt::Value::String(s) => {
                Ok(google_cloud_type::model::Decimal::new().set_value(s))
            }
            wkt::Value::Null => Err(ConversionError::NotNull),
            other => Err(ConversionError::TypeMismatch {
                expected: "string",
                got: other,
            }),
        }
    }
}



impl FromSql for wkt::Timestamp {
    fn from_sql(value: wkt::Value) -> Result<Self, ConversionError> {
        match value {
            wkt::Value::String(s) => {
                // Parse BQ microsecond epoch string (e.g. "1779982200000000" when useInt64Types is enabled)
                let val_f64 = s.parse::<f64>().map_err(|e| {
                    ConversionError::Convert(Box::new(e))
                })?;
                let micros = val_f64.trunc() as i64;
                let secs = micros / 1_000_000;
                let nanos = ((micros % 1_000_000) * 1000) as i32;
                wkt::Timestamp::new(secs, nanos).map_err(|e| {
                    ConversionError::Convert(Box::new(e))
                })
            }
            wkt::Value::Null => Err(ConversionError::NotNull),
            other => Err(ConversionError::TypeMismatch {
                expected: "string",
                got: other,
            }),
        }
    }
}

fn parse_date(s: &str) -> Result<(i32, i32, i32), BoxedError> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return Err("invalid date format".into());
    }
    let year = parts[0].parse::<i32>()?;
    let month = parts[1].parse::<i32>()?;
    let day = parts[2].parse::<i32>()?;
    Ok((year, month, day))
}

fn parse_time(s: &str) -> Result<(i32, i32, i32, i32), BoxedError> {
    let (time_str, nanos_str) = if let Some((t, n)) = s.split_once('.') {
        (t, Some(n))
    } else {
        (s, None)
    };
    let parts: Vec<&str> = time_str.split(':').collect();
    if parts.len() != 3 {
        return Err("invalid time format".into());
    }
    let hours = parts[0].parse::<i32>()?;
    let minutes = parts[1].parse::<i32>()?;
    let seconds = parts[2].parse::<i32>()?;
    
    let nanos = if let Some(n_str) = nanos_str {
        let mut padded = n_str.to_string();
        if padded.len() < 9 {
            padded.push_str(&"0".repeat(9 - padded.len()));
        } else if padded.len() > 9 {
            padded.truncate(9);
        }
        padded.parse::<i32>()?
    } else {
        0
    };
    Ok((hours, minutes, seconds, nanos))
}

fn parse_datetime(s: &str) -> Result<(i32, i32, i32, i32, i32, i32, i32), BoxedError> {
    let normalized = s.replace(' ', "T");
    let (date_str, time_str) = normalized.split_once('T').ok_or("invalid datetime format")?;
    let (year, month, day) = parse_date(date_str)?;
    let (hours, minutes, seconds, nanos) = parse_time(time_str)?;
    Ok((year, month, day, hours, minutes, seconds, nanos))
}

impl FromSql for google_cloud_type::model::Date {
    fn from_sql(value: wkt::Value) -> Result<Self, ConversionError> {
        match value {
            wkt::Value::String(s) => {
                let (year, month, day) = parse_date(&s).map_err(|e| {
                    ConversionError::Convert(e)
                })?;
                Ok(google_cloud_type::model::Date::new()
                    .set_year(year)
                    .set_month(month)
                    .set_day(day))
            }
            wkt::Value::Null => Err(ConversionError::NotNull),
            other => Err(ConversionError::TypeMismatch {
                expected: "string",
                got: other,
            }),
        }
    }
}

impl FromSql for google_cloud_type::model::TimeOfDay {
    fn from_sql(value: wkt::Value) -> Result<Self, ConversionError> {
        match value {
            wkt::Value::String(s) => {
                let (hours, minutes, seconds, nanos) = parse_time(&s).map_err(|e| {
                    ConversionError::Convert(e)
                })?;
                Ok(google_cloud_type::model::TimeOfDay::new()
                    .set_hours(hours)
                    .set_minutes(minutes)
                    .set_seconds(seconds)
                    .set_nanos(nanos))
            }
            wkt::Value::Null => Err(ConversionError::NotNull),
            other => Err(ConversionError::TypeMismatch {
                expected: "string",
                got: other,
            }),
        }
    }
}

impl FromSql for google_cloud_type::model::DateTime {
    fn from_sql(value: wkt::Value) -> Result<Self, ConversionError> {
        match value {
            wkt::Value::String(s) => {
                let (year, month, day, hours, minutes, seconds, nanos) = parse_datetime(&s).map_err(|e| {
                    ConversionError::Convert(e)
                })?;
                Ok(google_cloud_type::model::DateTime::new()
                    .set_year(year)
                    .set_month(month)
                    .set_day(day)
                    .set_hours(hours)
                    .set_minutes(minutes)
                    .set_seconds(seconds)
                    .set_nanos(nanos))
            }
            wkt::Value::Null => Err(ConversionError::NotNull),
            other => Err(ConversionError::TypeMismatch {
                expected: "string",
                got: other,
            }),
        }
    }
}

impl FromSql for wkt::Struct {
    fn from_sql(value: wkt::Value) -> Result<Self, ConversionError> {
        match value {
            wkt::Value::Object(obj) => Ok(obj),
            wkt::Value::Null => Err(ConversionError::NotNull),
            other => Err(ConversionError::TypeMismatch {
                expected: "object",
                got: other,
            }),
        }
    }
}

/// Implements deserialized_with trait
/// See: <https://serde.rs/custom-date-format.html>
pub fn deserialize<'de, D, T>(deserializer: D) -> std::result::Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: FromSql,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    T::from_sql(value).map_err(|e| serde::de::Error::custom(e.to_string()))
}
