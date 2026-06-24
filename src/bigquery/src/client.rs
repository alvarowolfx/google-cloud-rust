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

use crate::ClientBuilderResult as BuilderResult;
use crate::Result;
use crate::client_builder::ClientBuilder;
use crate::query::{Query, RunQuery};
use google_cloud_bigquery_v2::client::JobService;
use std::sync::Arc;

/// A high-level BigQuery client for executing queries and managing jobs.
#[derive(Clone, Debug)]
pub struct BigQuery {
    pub(crate) job_service: Arc<JobService>,
}

impl BigQuery {
    /// Convenient entrypoint to return a fresh configuration builder.
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    pub(crate) async fn new(builder: ClientBuilder) -> BuilderResult<Self> {
        let mut job_service_builder = JobService::builder();
        if let Some(creds) = builder.config.cred {
            job_service_builder = job_service_builder.with_credentials(creds);
        }
        if let Some(endpoint) = builder.config.endpoint {
            job_service_builder = job_service_builder.with_endpoint(endpoint);
        }
        if let Some(universe_domain) = builder.config.universe_domain {
            job_service_builder = job_service_builder.with_universe_domain(universe_domain);
        }
        if builder.config.tracing {
            job_service_builder = job_service_builder.with_tracing();
        }
        if let Some(retry_policy) = builder.config.retry_policy {
            job_service_builder = job_service_builder.with_retry_policy(retry_policy);
        }
        if let Some(backoff_policy) = builder.config.backoff_policy {
            job_service_builder = job_service_builder.with_backoff_policy(backoff_policy);
        }
        job_service_builder =
            job_service_builder.with_retry_throttler(builder.config.retry_throttler);
        let job_service = Arc::new(job_service_builder.build().await?);

        Ok(BigQuery { job_service })
    }

    /// Execute a SQL query.
    ///
    /// This builder internally routes to either `jobs.query` (fast path) or `jobs.insert` (job path)
    pub fn query<S: Into<String>>(&self, sql: S) -> RunQuery {
        RunQuery::new(self.job_service.clone(), sql.into())
    }

    /// Re-attach hook to bind an existing out-of-process job reference as a `Query` handle.
    pub async fn attach_job(
        &self,
        job_ref: google_cloud_bigquery_v2::model::JobReference,
    ) -> Result<Query> {
        let mut req = self
            .job_service
            .get_job()
            .set_job_id(job_ref.job_id.clone())
            .set_project_id(job_ref.project_id.clone());

        if let Some(location) = job_ref.location.clone() {
            req = req.set_location(location);
        }

        let job = req.send().await?;

        Ok(Query {
            job_service: self.job_service.clone(),
            job_ref: Some(job_ref),
            completed: false,
            initial_response: None,
            initial_job: Some(job),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::BigQuery;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;

    #[tokio::test]
    async fn test_bigquery_builder() -> anyhow::Result<()> {
        let _client = BigQuery::builder()
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;
        Ok(())
    }
}
