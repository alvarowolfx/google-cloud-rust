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
            .await?;

        let errors = res.errors.clone();
        if let Some(first_err) = errors.into_iter().next() {
            let rpc_status = google_cloud_gax::error::rpc::Status::default()
                .set_code(google_cloud_gax::error::rpc::Code::Unknown)
                .set_message(first_err.message);
            return Err(google_cloud_gax::error::Error::service(rpc_status));
        }

        let completed = res.job_complete.unwrap_or(false);
        let job_ref = res.job_reference.clone();

        Ok(Query {
            job_service: self.job_service.clone(),
            job_ref,
            completed,
            initial_response: Some(res),
            initial_job: None,
            is_job_path: false,
        })
    }
}

pub(crate) struct InsertJobExecutor {
    pub(crate) job_service: Arc<JobService>,
    pub(crate) job: Job,
    pub(crate) project_id: String,
}

impl InsertJobExecutor {
    pub(crate) async fn execute(self) -> Result<Query> {
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

        let insert_req = InsertJobRequest::new()
            .set_project_id(self.project_id.clone())
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
            is_job_path: true,
        })
    }
}
