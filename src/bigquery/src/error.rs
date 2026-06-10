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

//! Custom errors for the Cloud BigQuery query client.

use google_cloud_bigquery_v2::model::ErrorProto;
use google_cloud_gax::error::Error as GaxError;

/// Errors that can occur during query configuration, execution, or polling.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum QueryError {
    /// The project ID was not provided or could not be determined.
    #[error("no project ID was provided")]
    MissingProjectId,

    /// Only query jobs are supported by this client.
    #[error("only query jobs are supported")]
    UnsupportedJobType,

    /// The query job failed on the BigQuery service side.
    /// Includes the list of error protocols returned by the service.
    #[error("query job failed: {reason} - {message}")]
    JobFailed {
        /// The primary error reason code (e.g., "invalidQuery", "backendError").
        reason: String,
        /// The error message.
        message: String,
        /// The list of all errors associated with the job.
        errors: Vec<ErrorProto>,
    },

    /// The operation is not supported for stateless queries.
    #[error("cannot perform this operation on a stateless query")]
    StatelessQuery,

    /// An error occurred during service communications.
    #[error("service error: {0}")]
    Service(#[from] GaxError),
}

/// Errors that can occur when retrieving value cells from a [`Row`](crate::query::Row).
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum RowError {
    /// The requested column name or index was not found in the row.
    #[error("could not find column: {0}")]
    ColumnNotFound(String),

    /// The requested column index was out of range.
    #[error("column index out of range: {index} (expected < {len})")]
    IndexOutOfRange {
        /// The index that was requested.
        index: usize,
        /// The total number of columns in the row.
        len: usize,
    },

    /// Failed to convert/parse the cell value to the target type.
    #[error("type conversion error for column '{column}': {source}")]
    TypeConversion {
        /// The column identifier (name or index).
        column: String,
        /// The underlying parsing error.
        #[source]
        source: ConvertError,
    },

    /// The JSON format returned by the service did not match expectations.
    #[error("internal service JSON layout invalid: {0}")]
    InvalidRowFormat(String),
}

/// Represents failures when converting a raw BigQuery cell value (`wkt::Value`) to a Rust type.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum ConvertError {
    /// The value type did not match the expected type.
    #[error("type mismatch, expected {expected}, got {got:?}")]
    TypeMismatch {
        /// The expected type name.
        expected: &'static str,
        /// The actual value received.
        got: wkt::Value,
    },

    /// The value was null, but the target type does not support nulls (non-Option).
    #[error("expected non-null value, got null")]
    NotNull,

    /// An error occurred during custom conversion (e.g. parsing date/time strings).
    #[error("cannot convert value: {0}")]
    Convert(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
}
