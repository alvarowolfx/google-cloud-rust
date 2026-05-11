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

use crate::client::BigQuery;
use google_cloud_auth::credentials::Credentials;
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_gax::client_builder::Result;
use std::sync::Arc;

/// A builder for creating and configuring a BigQuery client instance.
pub struct ClientBuilder {
    pub(crate) project_id: Option<String>,
    pub(crate) credentials: Option<Credentials>,
    pub(crate) job_service: Option<Arc<JobService>>,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientBuilder {
    /// Creates a new default `ClientBuilder`.
    pub fn new() -> Self {
        Self {
            project_id: None,
            credentials: None,
            job_service: None,
        }
    }

    /// Sets the project ID for the client.
    pub fn with_project_id<V: Into<String>>(mut self, project_id: V) -> Self {
        self.project_id = Some(project_id.into());
        self
    }

    /// Sets custom credentials for the client.
    pub fn with_credentials<V: Into<Credentials>>(mut self, credentials: V) -> Self {
        self.credentials = Some(credentials.into());
        self
    }

    /// Sets a custom underlying `JobService` for the client.
    pub fn with_job_service(mut self, job_service: Arc<JobService>) -> Self {
        self.job_service = Some(job_service);
        self
    }

    /// Builds the `BigQuery` client instance.
    pub async fn build(self) -> Result<BigQuery> {
        let job_service = if let Some(js) = self.job_service {
            js
        } else {
            let mut builder = JobService::builder();
            if let Some(creds) = self.credentials {
                builder = builder.with_credentials(creds);
            }
            Arc::new(builder.build().await?)
        };

        let project_id = self
            .project_id
            .unwrap_or_else(|| std::env::var("GOOGLE_CLOUD_PROJECT").unwrap_or_default());

        Ok(BigQuery {
            job_service,
            project_id,
        })
    }
}
