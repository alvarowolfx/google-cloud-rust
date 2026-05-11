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
use crate::query::Query;
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::{InsertJobRequest, Job, JobReference, PostQueryRequest};
use std::collections::VecDeque;
use std::sync::Arc;

pub(crate) struct PostQueryExecutor {
    pub(crate) job_service: Arc<JobService>,
    pub(crate) billing_project: String,
    pub(crate) request: PostQueryRequest,
}

impl PostQueryExecutor {
    pub(crate) async fn execute(mut self) -> Result<Query> {
        if self.request.project_id.is_empty() {
            self.request.project_id = self.billing_project.clone();
        }

        let res = self
            .job_service
            .query()
            .with_request(self.request)
            .send()
            .await?;

        if let Some(first_err) = res.errors.into_iter().next() {
            let rpc_status = google_cloud_gax::error::rpc::Status::default()
                .set_code(google_cloud_gax::error::rpc::Code::Unknown)
                .set_message(first_err.message);
            return Err(google_cloud_gax::error::Error::service(rpc_status));
        }

        let completed = res.job_complete.unwrap_or(false);
        let total_rows = res.total_rows.unwrap_or(0);
        let num_dml_affected_rows = res.num_dml_affected_rows.unwrap_or(0);
        let schema = res.schema.map(Arc::new);
        let cached_rows = VecDeque::from(res.rows);
        let job_id = if let Some(ref jr) = res.job_reference {
            jr.job_id.clone()
        } else {
            res.query_id.clone()
        };

        Ok(Query {
            job_service: self.job_service.clone(),
            project_id: self.billing_project,
            job_id,
            location: res.location,
            completed,
            total_rows,
            num_dml_affected_rows,
            cached_rows,
            schema,
            page_token: res.page_token,
        })
    }
}

pub(crate) struct InsertJobExecutor {
    pub(crate) job_service: Arc<JobService>,
    pub(crate) billing_project: String,
    pub(crate) job: Job,
}

impl InsertJobExecutor {
    pub(crate) async fn execute(mut self) -> Result<Query> {
        let is_query = self
            .job
            .configuration
            .as_ref()
            .and_then(|c| c.query.as_ref())
            .is_some();
        if !is_query {
            let rpc_status = google_cloud_gax::error::rpc::Status::default()
                .set_code(google_cloud_gax::error::rpc::Code::InvalidArgument)
                .set_message("Only Query Jobs are supported by this client.");
            return Err(google_cloud_gax::error::Error::service(rpc_status));
        }

        if self.job.job_reference.is_none() {
            self.job.job_reference =
                Some(JobReference::new().set_project_id(self.billing_project.clone()));
        } else if let Some(ref mut jr) = self.job.job_reference {
            if jr.project_id.is_empty() {
                jr.project_id = self.billing_project.clone();
            }
        }

        let insert_req = InsertJobRequest::new()
            .set_project_id(self.billing_project.clone())
            .set_job(self.job);

        let res = self
            .job_service
            .insert_job()
            .with_request(insert_req)
            .send()
            .await?;

        if let Some(ref status) = res.status {
            if let Some(ref err) = status.error_result {
                let rpc_status = google_cloud_gax::error::rpc::Status::default()
                    .set_code(google_cloud_gax::error::rpc::Code::Unknown)
                    .set_message(err.message.clone());
                return Err(google_cloud_gax::error::Error::service(rpc_status));
            }
        }

        let completed = res
            .status
            .as_ref()
            .map(|s| s.state == "DONE")
            .unwrap_or(false);
        let (job_id, location) = if let Some(ref jr) = res.job_reference {
            (jr.job_id.clone(), jr.location.clone().unwrap_or_default())
        } else {
            (res.id.clone(), String::new())
        };

        Ok(Query {
            job_service: self.job_service.clone(),
            project_id: self.billing_project,
            job_id,
            location,
            completed,
            total_rows: 0,
            num_dml_affected_rows: 0,
            cached_rows: VecDeque::new(),
            schema: None,
            page_token: String::new(),
        })
    }
}
