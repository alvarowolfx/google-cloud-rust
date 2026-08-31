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

//! BigQuery I/O Plugin for Polars using Arrow C Stream Interface and Storage Read API acceleration.

use google_cloud_auth::credentials::{
    CacheableResource, Credentials, CredentialsProvider, EntityTag,
};
use google_cloud_auth::errors::CredentialsError;
use google_cloud_bigquery::client::BigQuery;
use google_cloud_bigquery::query::ArrowArrayStream;
use http::header::AUTHORIZATION;
use http::{HeaderMap, HeaderValue};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use std::collections::HashMap;
use std::ffi::CString;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Bridges Polars' Python CredentialProvider (e.g., `pl.CredentialProviderGCP` or custom callable)
/// into the Rust SDK's `CredentialsProvider` trait without any internal changes to `google-cloud-rust`.
#[derive(Debug)]
pub struct PyPolarsCredentialProvider {
    py_provider: Arc<PyObject>,
    quota_project_id: Option<String>,
    cached_headers: Arc<tokio::sync::RwLock<Option<(HeaderMap, Option<Instant>)>>>,
}

impl PyPolarsCredentialProvider {
    pub fn new(py_provider: PyObject, quota_project_id: Option<String>) -> Self {
        Self {
            py_provider: Arc::new(py_provider),
            quota_project_id,
            cached_headers: Arc::new(tokio::sync::RwLock::new(None)),
        }
    }
}

impl CredentialsProvider for PyPolarsCredentialProvider {
    async fn headers(
        &self,
        _extensions: http::Extensions,
    ) -> Result<CacheableResource<HeaderMap>, CredentialsError> {
        // Fast path: check in-memory cached headers before hitting Python / acquiring GIL.
        {
            let lock = self.cached_headers.read().await;
            if let Some((ref headers, ref expiry)) = *lock {
                let is_valid = expiry.map(|exp| Instant::now() < exp).unwrap_or(true);
                if is_valid {
                    return Ok(CacheableResource::New {
                        entity_tag: EntityTag::new(),
                        data: headers.clone(),
                    });
                }
            }
        }

        let provider = self.py_provider.clone();

        // Safely invoke the Python credential provider off the Tokio async worker thread.
        let (bearer_token, expiry_timestamp) = tokio::task::spawn_blocking(
            move || -> Result<(String, Option<i64>), CredentialsError> {
                Python::with_gil(|py| {
                    let py_obj = provider.bind(py);

                    // Polars CredentialProvider classes implement `retrieve_credentials_impl` or `__call__`
                    let res = if py_obj.hasattr("retrieve_credentials_impl").unwrap_or(false) {
                        py_obj
                            .call_method0("retrieve_credentials_impl")
                            .map_err(|e| CredentialsError::from_msg(false, e.to_string()))?
                    } else if py_obj.is_callable() {
                        py_obj
                            .call0()
                            .map_err(|e| CredentialsError::from_msg(false, e.to_string()))?
                    } else {
                        py_obj.clone()
                    };

                    // Polars CredentialProvider returns a tuple of (credentials_dict, expiry)
                    let (creds_dict, expiry_ts) =
                        if let Ok(tuple) = res.downcast::<pyo3::types::PyTuple>() {
                            let dict = tuple
                                .get_item(0)
                                .map_err(|e| CredentialsError::from_msg(false, e.to_string()))?;
                            let exp = if tuple.len() > 1 {
                                tuple.get_item(1).ok().and_then(|v| v.extract::<i64>().ok())
                            } else {
                                None
                            };
                            (dict, exp)
                        } else if let Ok(list) = res.downcast::<pyo3::types::PyList>() {
                            let dict = list
                                .get_item(0)
                                .map_err(|e| CredentialsError::from_msg(false, e.to_string()))?;
                            let exp = if list.len() > 1 {
                                list.get_item(1).ok().and_then(|v| v.extract::<i64>().ok())
                            } else {
                                None
                            };
                            (dict, exp)
                        } else {
                            (res, None)
                        };

                    let bearer_token: String = if let Ok(token_item) = creds_dict.get_item("bearer_token") {
                        token_item.extract().map_err(|e| CredentialsError::from_msg(false, e.to_string()))?
                    } else if let Ok(token_item) = creds_dict.get_item("token") {
                        token_item.extract().map_err(|e| CredentialsError::from_msg(false, e.to_string()))?
                    } else {
                        return Err(CredentialsError::from_msg(
                            false,
                            format!("CredentialProvider must return a dictionary containing 'bearer_token' or 'token', got: {creds_dict:?}"),
                        ));
                    };

                    Ok((bearer_token, expiry_ts))
                })
            },
        )
        .await
        .map_err(|e| CredentialsError::from_msg(false, e.to_string()))??;

        let mut headers = HeaderMap::new();
        let auth_val = HeaderValue::from_str(&format!("Bearer {bearer_token}"))
            .map_err(|e| CredentialsError::from_msg(false, e.to_string()))?;
        headers.insert(AUTHORIZATION, auth_val);

        if let Some(ref proj) = self.quota_project_id
            && let Ok(val) = HeaderValue::from_str(proj)
        {
            headers.insert("x-goog-user-project", val);
        }

        // Cache token in memory until ~60s before expiry
        let now_epoch_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let valid_until = if let Some(exp) = expiry_timestamp {
            let diff = exp.saturating_sub(now_epoch_secs);
            let cache_secs = (diff.saturating_sub(60)).max(10) as u64;
            Some(Instant::now() + Duration::from_secs(cache_secs))
        } else {
            Some(Instant::now() + Duration::from_secs(3000))
        };

        {
            let mut write_lock = self.cached_headers.write().await;
            *write_lock = Some((headers.clone(), valid_until));
        }

        Ok(CacheableResource::New {
            entity_tag: EntityTag::new(),
            data: headers,
        })
    }

    async fn universe_domain(&self) -> Option<String> {
        Some("googleapis.com".to_string())
    }
}

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

static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to initialize Tokio runtime for BigQuery Polars plugin")
});

#[derive(Clone, Hash, PartialEq, Eq)]
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

/// The Python module definition for `_polars_bigquery`.
#[pymodule]
fn _polars_bigquery(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<BigQueryArrowStream>()?;
    m.add_function(wrap_pyfunction!(execute_bigquery_stream, m)?)?;
    Ok(())
}
