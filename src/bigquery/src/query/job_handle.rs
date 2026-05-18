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

use crate::Result;
use crate::query::job_reference::JobReference;
use crate::query::query_handle::poll_query_results;
use crate::query::{ReadRequest, Schema};
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::{GetQueryResultsResponse, Job};
use std::collections::VecDeque;
use std::sync::Arc;

/// A handle representing a running or finished background query job request (`jobs.insert`).
#[derive(Debug, Clone)]
pub struct QueryJob {
    pub(crate) job_service: Arc<JobService>,
    pub(crate) job_ref: JobReference,
    pub(crate) initial_job: Job,
}

impl QueryJob {
    /// Periodically checks the status of the background job until it finishes.
    /// Returns an error if a remote service or connection failure happens during polling.
    pub async fn until_done(&self) -> Result<CompleteQueryJob> {
        let res = poll_query_results(&self.job_service, &self.job_ref).await?;
        let Some(job_ref) = self.job_ref.to_job_ref() else {
            return Err(google_cloud_gax::error::Error::io("invalid job reference"));
        };

        // To populate the complete QueryJobMetadata with the full Job configuration and status,
        // we also fetch the Job resource from the server.
        let mut job_req = self
            .job_service
            .get_job()
            .set_job_id(job_ref.job_id)
            .set_project_id(job_ref.project_id);

        if let Some(location) = job_ref.location {
            job_req = job_req.set_location(location);
        }
        let completed_job = job_req.send().await?;

        Ok(CompleteQueryJob::from_job_and_results(
            self,
            completed_job,
            res,
        ))
    }

    /// Returns the underlying job reference for this query job.
    pub fn job_reference(&self) -> Option<google_cloud_bigquery_v2::model::JobReference> {
        self.job_ref.to_job_ref()
    }

    /// Returns the initial raw `Job` received from the service.
    pub fn metadata(&self) -> &Job {
        &self.initial_job
    }
}

/// A handle representing a successfully completed query job ready for reading.
#[derive(Debug, Clone)]
pub struct CompleteQueryJob {
    pub(crate) job_service: Arc<JobService>,
    pub(crate) job_ref: JobReference,
    pub(crate) cached_rows: VecDeque<wkt::Struct>,
    pub(crate) schema: Arc<Schema>,
    pub(crate) page_token: Option<String>,
    pub(crate) metadata: Job,
}

impl CompleteQueryJob {
    pub(crate) fn from_job_and_results(
        q: &QueryJob,
        complete_job: Job,
        res: GetQueryResultsResponse,
    ) -> Self {
        let schema = res
            .schema
            .clone()
            .expect("complete query job should have schema");
        let schema = Arc::new(schema);
        let page_token = if res.page_token.is_empty() {
            None
        } else {
            Some(res.page_token.clone())
        };
        let cached_rows = VecDeque::from(res.rows.clone());
        Self {
            job_service: q.job_service.clone(),
            job_ref: q.job_ref.clone(),
            cached_rows,
            page_token,
            schema,
            metadata: complete_job,
        }
    }

    /// Transitions the completed query job into a paginated row stream.
    pub fn read(self) -> ReadRequest {
        self.into()
    }

    /// Returns the cached metadata for this completed query job.
    pub fn metadata(&self) -> &Job {
        &self.metadata
    }
}
