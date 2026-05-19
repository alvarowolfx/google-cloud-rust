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
use crate::query::{CompleteQuery, ReadRequest, Schema};
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
    pub async fn until_done(&self) -> Result<CompleteQuery> {
        let res = poll_query_results(&self.job_service, &self.job_ref).await?;
        Ok(CompleteQuery::from_query_job_and_results(self, res))
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
