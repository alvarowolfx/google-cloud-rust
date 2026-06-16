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

use crate::error::QueryError;
use crate::query::{Query, Result};
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::{InsertJobRequest, Job, PostQueryRequest};
use std::sync::Arc;

pub(crate) struct PostQueryExecutor {
    pub(crate) job_service: Arc<JobService>,
    pub(crate) request: PostQueryRequest,
}

impl PostQueryExecutor {
    pub(crate) async fn execute(self) -> Result<Query> {
        let res = self
            .job_service
            .query()
            .with_request(self.request)
            .send()
            .await
            .map_err(|e| QueryError::Rpc { source: e })?;

        if !res.errors.is_empty() {
            return Err(QueryError::JobFailed { errors: res.errors });
        }

        let completed = res.job_complete.unwrap_or(false);
        let job_ref = res.job_reference.clone();

        Ok(Query {
            job_service: self.job_service.clone(),
            job_ref,
            completed,
            initial_response: Some(res),
            initial_job: None,
        })
    }
}

pub(crate) struct InsertJobExecutor {
    pub(crate) job_service: Arc<JobService>,
    pub(crate) request: InsertJobRequest,
}

impl InsertJobExecutor {
    pub(crate) async fn execute(self) -> Result<Query> {
        let is_query = self
            .request
            .job
            .as_ref()
            .and_then(|job| job.configuration.as_ref())
            .and_then(|c| c.query.as_ref())
            .is_some();
        if !is_query {
            return Err(QueryError::UnsupportedJobType);
        }

        let res = self
            .job_service
            .insert_job()
            .with_request(self.request)
            .send()
            .await
            .map_err(|e| QueryError::Rpc { source: e })?;

        let job_status = res.status.as_ref();
        if let Some(_) = job_status.and_then(|s| s.error_result.as_ref()) {
            let errors = job_status.map(|s| s.errors.clone()).unwrap_or_default();
            return Err(QueryError::JobFailed { errors });
        }

        let job_ref = res
            .job_reference
            .clone()
            .expect("newly inserted job should have job reference");
        Ok(Query {
            job_service: self.job_service.clone(),
            job_ref: Some(job_ref),
            completed: false,
            initial_response: None,
            initial_job: Some(res),
        })
    }
}
