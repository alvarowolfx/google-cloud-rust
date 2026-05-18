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
use crate::query::execution::{InsertJobExecutor, PostQueryExecutor};
use crate::query::{Query, QueryJob};
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::{Job, PostQueryRequest};
use std::sync::Arc;

/// A request builder for configuring and running a fast SQL query (`jobs.query`).
#[derive(Debug, Clone)]
pub struct RunQuery {
    pub(crate) job_service: Arc<JobService>,
    pub(crate) client_project_id: String,
    pub(crate) request: PostQueryRequest,
    pub(crate) billing_project_id: Option<String>,
}

impl RunQuery {
    /// Sets an optional billing project ID to override the default client project ID where the query runs.
    pub fn set_billing_project_id<S: Into<String>>(mut self, project_id: S) -> Self {
        self.billing_project_id = Some(project_id.into());
        self
    }

    /// Executes the configured query request.
    pub async fn run(self) -> Result<Query> {
        let billing_project = self.billing_project_id.unwrap_or(self.client_project_id);
        PostQueryExecutor {
            job_service: self.job_service,
            billing_project,
            request: self.request,
        }
        .execute()
        .await
    }
}

/// A request builder for configuring and running an advanced background query job (`jobs.insert`).
#[derive(Debug, Clone)]
pub struct RunQueryJob {
    pub(crate) job_service: Arc<JobService>,
    pub(crate) client_project_id: String,
    pub(crate) job: Job,
    pub(crate) billing_project_id: Option<String>,
}

impl RunQueryJob {
    /// Sets an optional billing project ID to override the default client project ID where the job runs.
    pub fn set_billing_project_id<S: Into<String>>(mut self, project_id: S) -> Self {
        self.billing_project_id = Some(project_id.into());
        self
    }

    /// Executes the configured background query job.
    pub async fn run(self) -> Result<QueryJob> {
        let billing_project = self.billing_project_id.unwrap_or(self.client_project_id);
        InsertJobExecutor {
            job_service: self.job_service,
            billing_project,
            job: self.job,
        }
        .execute()
        .await
    }
}
