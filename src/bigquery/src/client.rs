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
use crate::query::{IntoJob, IntoPostQueryRequest, QueryJob, RunQuery, RunQueryJob};
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
        let job_service = Arc::new(job_service_builder.build().await?);

        Ok(BigQuery { job_service })
    }

    /// Prepares a fast-path query execution by returning a `RunQuery` builder.
    pub fn query<T: IntoPostQueryRequest>(&self, req: T) -> RunQuery {
        RunQuery {
            job_service: self.job_service.clone(),
            request: req.into_post_query_request(),
        }
    }

    /// Prepares a background query job execution by returning a `RunQueryJob` builder.
    pub fn query_job<T: IntoJob>(&self, req: T) -> RunQueryJob {
        RunQueryJob {
            job_service: self.job_service.clone(),
            job: req.into_job(),
            project_id: None,
        }
    }

    /// Re-attach hook to bind an existing out-of-process job reference as a `QueryJob`.
    pub async fn attach_job(
        &self,
        job_ref: google_cloud_bigquery_v2::model::JobReference,
    ) -> Result<QueryJob> {
        let mut req = self
            .job_service
            .get_job()
            .set_job_id(job_ref.job_id.clone())
            .set_project_id(job_ref.project_id.clone());

        if let Some(location) = job_ref.location.clone() {
            req = req.set_location(location);
        }

        let job = req.send().await?;

        Ok(QueryJob {
            job_service: self.job_service.clone(),
            job_ref: Some(job_ref),
            initial_job: job,
        })
    }
}
