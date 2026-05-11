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

use crate::client_builder::ClientBuilder;
use crate::query::{Query, QueryRequest, RunQuery};
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::JobReference;
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
}

impl BigQuery {
    /// Path 3: Re-attach hook to bind an existing out-of-process job reference.
    pub fn attach_job(&self, job_ref: JobReference) -> Query {
        let project_id = if job_ref.project_id.is_empty() {
            self.project_id.clone()
        } else {
            job_ref.project_id.clone()
        };
        let location = job_ref.location.clone().unwrap_or_default();

        Query {
            job_service: self.job_service.clone(),
            project_id,
            job_id: job_ref.job_id,
            location,
            completed: false,
            total_rows: 0,
            num_dml_affected_rows: 0,
            cached_rows: std::collections::VecDeque::new(),
            schema: None,
            page_token: String::new(),
        }
    }
}
