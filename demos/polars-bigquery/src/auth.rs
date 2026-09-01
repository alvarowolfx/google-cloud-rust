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

//! Authentication adapters bridging Polars Python credential providers to Google Cloud Rust credentials.

use google_cloud_auth::credentials::{CacheableResource, CredentialsProvider, EntityTag};
use google_cloud_auth::errors::CredentialsError;
use http::header::AUTHORIZATION;
use http::{HeaderMap, HeaderValue};
use pyo3::prelude::*;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

type CachedHeaders = Arc<tokio::sync::RwLock<Option<(HeaderMap, Option<Instant>)>>>;

/// Bridges Polars' Python CredentialProvider (e.g., `pl.CredentialProviderGCP` or custom callable)
/// into the Rust SDK's `CredentialsProvider` trait without any internal changes to `google-cloud-rust`.
#[derive(Debug)]
pub struct PyPolarsCredentialProvider {
    py_provider: Arc<PyObject>,
    quota_project_id: Option<String>,
    cached_headers: CachedHeaders,
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
