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
use crate::query::{JobReference, Query, QueryCreationMetadata, QueryRequest, RunQuery};
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_gax::client_builder::Error as ClientBuilderError;
use std::sync::Arc;

/// A high-level BigQuery client for executing queries and managing jobs.
#[derive(Clone, Debug)]
pub struct BigQuery {
    pub(crate) job_service: Arc<JobService>,
    pub(crate) project_id: String,
}

impl BigQuery {
    /// Convenient entrypoint to return a fresh configuration builder.
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    pub(crate) async fn new(builder: ClientBuilder) -> BuilderResult<Self> {
        let job_service = if let Some(service) = builder.job_service {
            service
        } else {
            let mut job_service_builder = JobService::builder();
            if let Some(creds) = builder.config.cred {
                job_service_builder = job_service_builder.with_credentials(creds);
            }
            let job_service = job_service_builder.build().await?;
            Arc::new(job_service)
        };

        let project_id = builder
            .project_id
            .or_else(|| std::env::var("GOOGLE_CLOUD_PROJECT").ok())
            .ok_or_else(|| ClientBuilderError::cred("missing project id"))?;

        Ok(BigQuery {
            job_service,
            project_id,
        })
    }

    /// Prepares a query execution by returning a `RunQuery` to configure additional options.
    pub fn query<T>(&self, req: T) -> RunQuery
    where
        T: Into<QueryRequest>,
    {
        RunQuery {
            job_service: self.job_service.clone(),
            client_project_id: self.project_id.clone(),
            request: req.into(),
            billing_project_id: None,
        }
    }

    /// Re-attach hook to bind an existing out-of-process job reference.
    pub async fn attach_job(
        &self,
        job_ref: google_cloud_bigquery_v2::model::JobReference,
    ) -> Result<Query> {
        let internal_job_ref: JobReference = job_ref.clone().into();
        let mut req = self
            .job_service
            .get_job()
            .set_job_id(job_ref.job_id)
            .set_project_id(job_ref.project_id);

        if let Some(location) = job_ref.location {
            req = req.set_location(location);
        }

        let job = req.send().await?;

        Ok(Query {
            job_service: self.job_service.clone(),
            job_ref: internal_job_ref,
            completed: false,
            metadata: QueryCreationMetadata::JobsInsert(job),
        })
    }
}
