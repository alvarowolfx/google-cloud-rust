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
use crate::query::metadata::QueryMetadata;
use crate::query::{RowIterator, Schema};
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::{
    GetQueryResultsRequest, GetQueryResultsResponse, Job, JobReference, QueryResponse,
};
use std::collections::VecDeque;
use std::sync::Arc;

/// A handle representing a running query job or stateless query execution.
#[derive(Debug, Clone)]
pub struct Query {
    pub(crate) job_service: Arc<JobService>,
    pub(crate) job_ref: Option<JobReference>,
    pub(crate) completed: bool,
    pub(crate) initial_response: Option<QueryResponse>,
    pub(crate) initial_job: Option<Job>,
    pub(crate) is_job_path: bool,
}

impl Query {
    /// Periodically checks the status of the background job until it finishes.
    /// Returns an error if a remote service or connection failure happens during polling.
    pub async fn until_done(&self) -> Result<CompleteQuery> {
        if self.completed {
            if let Some(ref initial_response) = self.initial_response {
                return Ok(CompleteQuery::from_query_response(self, initial_response));
            }
        }

        let res = poll_query_results(&self.job_service, self.job_ref.as_ref()).await?;
        Ok(CompleteQuery::from_get_query_results_response(self, res))
    }

    /// Returns the stateless query ID of the query, if available.
    pub fn query_id(&self) -> Option<String> {
        self.initial_response.as_ref().and_then(|res| {
            if res.query_id.is_empty() {
                None
            } else {
                Some(res.query_id.clone())
            }
        })
    }

    /// Returns the underlying job reference for this query.
    pub fn job_reference(&self) -> Option<google_cloud_bigquery_v2::model::JobReference> {
        self.job_ref.clone()
    }
}

/// A handle representing a successfully completed query ready for reading.
#[derive(Debug, Clone)]
pub struct CompleteQuery {
    pub(crate) job_service: Arc<JobService>,
    pub(crate) job_ref: Option<JobReference>,
    pub(crate) cached_rows: VecDeque<wkt::Struct>,
    pub(crate) schema: Arc<Schema>,
    pub(crate) page_token: Option<String>,
    pub(crate) metadata: QueryMetadata,
}

impl CompleteQuery {
    pub(crate) fn from_get_query_results_response(q: &Query, res: GetQueryResultsResponse) -> Self {
        let schema = res
            .schema
            .clone()
            .expect("complete query should have schema");
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
            metadata: QueryMetadata::GetQueryResultsResponse(res),
        }
    }

    pub(crate) fn from_query_response(q: &Query, res: &QueryResponse) -> Self {
        let schema = res
            .schema
            .clone()
            .expect("complete query should have schema");
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
            metadata: QueryMetadata::JobsQuery(res.clone()),
        }
    }

    /// Returns a row iterator for the query result.
    pub fn read(self) -> RowIterator {
        RowIterator::new(self)
    }

    /// Returns the cached metadata for this query.
    pub fn metadata(&self) -> &QueryMetadata {
        &self.metadata
    }

    /// Performs a network call to fetch the full `Job` resource from the backend.
    pub async fn job_metadata(&self) -> Result<Job> {
        let job_ref = self.job_ref.as_ref().ok_or_else(|| {
            google_cloud_gax::error::Error::io("cannot fetch job metadata for stateless queries")
        })?;

        let mut req = self
            .job_service
            .get_job()
            .set_job_id(job_ref.job_id.clone())
            .set_project_id(job_ref.project_id.clone());

        if let Some(location) = job_ref.location.clone() {
            req = req.set_location(location);
        }

        req.send()
            .await
            .map_err(|_| google_cloud_gax::error::Error::io("failing to get job metadata"))
    }
}

/// Helper function to poll getQueryResults until a job finishes.
pub(crate) async fn poll_query_results(
    job_service: &JobService,
    job_ref: Option<&JobReference>,
) -> Result<GetQueryResultsResponse> {
    loop {
        let job_ref = job_ref
            .ok_or_else(|| google_cloud_gax::error::Error::io("can't poll stateless queries"))?;

        let mut req = GetQueryResultsRequest::new()
            .set_project_id(job_ref.project_id.clone())
            .set_job_id(job_ref.job_id.clone());
        if let Some(location) = job_ref.location.clone() {
            req = req.set_location(location);
        }

        let res = job_service
            .get_query_results()
            .with_request(req)
            .send()
            .await?;

        if let Some(first_err) = res.errors.clone().into_iter().next() {
            let rpc_status = google_cloud_gax::error::rpc::Status::default()
                .set_code(google_cloud_gax::error::rpc::Code::Unknown)
                .set_message(first_err.message);
            return Err(google_cloud_gax::error::Error::service(rpc_status));
        }

        let completed = res.job_complete.unwrap_or(false);
        if completed {
            return Ok(res);
        }
    }
}
