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

//! BigQuery I/O Plugin for Polars using Arrow C Stream Interface and Storage Read/Write API acceleration.

pub mod auth;
pub mod query;
pub mod write;

pub use auth::PyPolarsCredentialProvider;

use pyo3::prelude::*;
use std::sync::LazyLock;

pub(crate) static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to initialize Tokio runtime for BigQuery Polars plugin")
});

/// The Python module definition for `_polars_bigquery`.
#[pymodule]
fn _polars_bigquery(m: &Bound<'_, PyModule>) -> PyResult<()> {
    query::register_query_module(m)?;
    write::register_write_module(m)?;
    Ok(())
}
