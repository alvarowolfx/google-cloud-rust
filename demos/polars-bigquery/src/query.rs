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

//! BigQuery query execution and Arrow C Stream reader for Polars with Storage Read API acceleration.

use crate::RUNTIME;
use crate::auth::PyPolarsCredentialProvider;
use google_cloud_auth::credentials::Credentials;
use google_cloud_bigquery::client::BigQuery;
use google_cloud_bigquery::query::ArrowArrayStream;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use std::collections::HashMap;
use std::ffi::CString;
use std::sync::LazyLock;

/// A Python wrapper for an active BigQuery Arrow C Stream that implements the Arrow PyCapsule protocol (`__arrow_c_stream__`).
#[pyclass(name = "BigQueryArrowStream")]
pub struct BigQueryArrowStream {
    stream: Option<ArrowArrayStream>,
}

// ArrowArrayStream holds raw pointers for C ABI; it is safe to transfer between threads and Python.
unsafe impl Send for BigQueryArrowStream {}
unsafe impl Sync for BigQueryArrowStream {}

#[pymethods]
impl BigQueryArrowStream {
    /// Implements the Arrow PyCapsule protocol for zero-copy streaming into Polars, DuckDB, or PyArrow.
    #[pyo3(signature = (_requested_schema=None))]
    pub fn __arrow_c_stream__<'py>(
        &mut self,
        py: Python<'py>,
        _requested_schema: Option<PyObject>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let stream = self.stream.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Arrow C Stream has already been consumed",
            )
        })?;

        let capsule_name = CString::new("arrow_array_stream")
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        PyCapsule::new_with_destructor(
            py,
            stream,
            Some(capsule_name),
            |stream: ArrowArrayStream, _ptr: *mut std::ffi::c_void| {
                drop(stream);
            },
        )
    }
}

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
enum ClientCacheKey {
    Adc {
        project_id: Option<String>,
        storage_read: bool,
    },
    Custom {
        provider_ptr: usize,
        project_id: Option<String>,
        storage_read: bool,
    },
}

static CLIENT_CACHE: LazyLock<tokio::sync::RwLock<HashMap<ClientCacheKey, BigQuery>>> =
    LazyLock::new(|| tokio::sync::RwLock::new(HashMap::new()));

/// Executes a BigQuery query asynchronously and returns a `BigQueryArrowStream` conforming to the Arrow C Stream PyCapsule interface.
#[pyfunction]
#[pyo3(signature = (query, project_id=None, credential_provider=None, storage_read=true))]
pub fn execute_bigquery_stream(
    py: Python<'_>,
    query: String,
    project_id: Option<String>,
    credential_provider: Option<PyObject>,
    storage_read: bool,
) -> PyResult<BigQueryArrowStream> {
    py.allow_threads(|| {
        RUNTIME.block_on(async move {
            let key = if let Some(ref provider) = credential_provider {
                ClientCacheKey::Custom {
                    provider_ptr: provider.as_ptr() as usize,
                    project_id: project_id.clone(),
                    storage_read,
                }
            } else {
                ClientCacheKey::Adc {
                    project_id: project_id.clone(),
                    storage_read,
                }
            };

            let cached_client = {
                let lock = CLIENT_CACHE.read().await;
                lock.get(&key).cloned()
            };

            let client = match cached_client {
                Some(c) => c,
                None => {
                    let mut builder = BigQuery::builder().with_storage_read(storage_read);

                    if let Some(ref proj) = project_id {
                        builder = builder.with_project_id(proj);
                    }

                    if let Some(provider) = credential_provider {
                        let auth_provider =
                            PyPolarsCredentialProvider::new(provider, project_id.clone());
                        builder = builder.with_credentials(Credentials::from(auth_provider));
                    }

                    let new_client = builder.build().await.map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                    })?;

                    let mut lock = CLIENT_CACHE.write().await;
                    lock.entry(key)
                        .or_insert_with(|| new_client.clone())
                        .clone()
                }
            };

            let mut q = client.query(&query);
            if let Some(ref proj) = project_id {
                q = q.with_project_id(proj);
            }

            let complete_query = q
                .until_done()
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            let c_stream = complete_query
                .to_arrow_c_stream()
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Ok(BigQueryArrowStream {
                stream: Some(c_stream),
            })
        })
    })
}

/// Registers query module functions and types on the Python extension module.
pub fn register_query_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<BigQueryArrowStream>()?;
    m.add_function(wrap_pyfunction!(execute_bigquery_stream, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_cache_key() {
        let k1 = ClientCacheKey::Adc {
            project_id: Some("p1".into()),
            storage_read: true,
        };
        let k2 = ClientCacheKey::Adc {
            project_id: Some("p1".into()),
            storage_read: true,
        };
        let k3 = ClientCacheKey::Adc {
            project_id: Some("p1".into()),
            storage_read: false,
        };
        assert_eq!(k1, k2);
        assert_ne!(k1, k3);
    }
}
