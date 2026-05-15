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
use crate::query::{JobReference, Row, RowIterator, Schema};
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::{
    GetQueryResultsRequest, GetQueryResultsResponse, QueryResponse,
};
use std::collections::VecDeque;
use std::sync::Arc;

/// Errors that can happen when managing queries.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Raised when trying to read data rows before the background job finishes successfully.
    #[error("Query is not complete: Only complete Query Jobs can be read.")]
    NotComplete,
}

/// A handle representing a running or finished BigQuery query job.
#[derive(Debug, Clone)]
pub struct Query {
    pub(crate) job_service: Arc<JobService>,
    pub(crate) job_ref: JobReference,
    pub(crate) completed: bool,
    pub(crate) creation_metadata: QueryCreationMetadata,
}

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum QueryCreationMetadata {
    JobsQuery(google_cloud_bigquery_v2::model::QueryResponse),
    JobsInsert(google_cloud_bigquery_v2::model::Job),
}

impl Query {
    /// Periodically checks the status of the background job until it finishes.
    /// Returns an error if a remote service or connection failure happens during polling.
    pub async fn until_done(&self) -> Result<CompleteQuery> {
        if self.completed {
            return Ok(CompleteQuery::from_complete_query(self));
        }

        loop {
            let Some(job_ref) = self.job_ref.as_job_ref() else {
                return Err(google_cloud_gax::error::Error::io(
                    "can't poll stateless queries",
                ));
            };

            let mut req = GetQueryResultsRequest::new()
                .set_project_id(job_ref.project_id.clone())
                .set_job_id(job_ref.job_id.clone());
            if let Some(location) = job_ref.location.clone() {
                req = req.set_location(location);
            }

            let res = self
                .job_service
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
                return Ok(CompleteQuery::from_get_query_results_response(self, res));
            }
        }
    }

    pub fn creation_metadata(&self) -> &QueryCreationMetadata {
        &self.creation_metadata
    }

    /// Returns the query id of the query if it was created as a stateless query.
    pub fn query_id(&self) -> Option<String> {
        match self.job_ref {
            JobReference::StatelessJob { ref query_id } => Some(query_id.clone()),
            _ => None,
        }
    }

    /// Returns the underlying job reference for this query.
    pub fn job_reference(&self) -> Option<google_cloud_bigquery_v2::model::JobReference> {
        self.job_ref.as_job_ref()
    }
}

#[derive(Debug, Clone)]
pub struct CompleteQuery {
    pub(crate) job_service: Arc<JobService>,
    pub(crate) job_ref: JobReference,
    pub(crate) cached_rows: VecDeque<wkt::Struct>,
    pub(crate) schema: Arc<Schema>,
    pub(crate) page_token: Option<String>,
    pub(crate) query_metadata: QueryMetadata,
}

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum QueryMetadata {
    JobsQuery(google_cloud_bigquery_v2::model::QueryResponse),
    GetQueryResultsResponse(google_cloud_bigquery_v2::model::GetQueryResultsResponse),
}

impl QueryMetadata {
    /// Returns the total number of rows in the complete query result set.
    pub fn total_rows(&self) -> u64 {
        match self {
            QueryMetadata::GetQueryResultsResponse(res) => res.total_rows.unwrap_or(0),
            QueryMetadata::JobsQuery(res) => res.total_rows.unwrap_or(0),
        }
    }

    /// Returns the total number of rows in the complete query result set.
    pub fn num_dml_affected_rows(&self) -> i64 {
        match self {
            QueryMetadata::GetQueryResultsResponse(res) => res.num_dml_affected_rows.unwrap_or(0),
            QueryMetadata::JobsQuery(res) => res.num_dml_affected_rows.unwrap_or(0),
        }
    }
}

impl CompleteQuery {
    fn from_complete_query(q: &Query) -> Self {
        let res = match q.creation_metadata.clone() {
            QueryCreationMetadata::JobsQuery(res) => res,
            _ => unreachable!("running queries via jobs.insert are not gonna be complete"),
        };

        Self::from_query_response(q, &res)
    }

    fn from_get_query_results_response(q: &Query, res: GetQueryResultsResponse) -> Self {
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
            query_metadata: QueryMetadata::GetQueryResultsResponse(res.clone()),
        }
    }

    fn from_query_response(q: &Query, res: &QueryResponse) -> Self {
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
            query_metadata: QueryMetadata::JobsQuery(res.clone()),
        }
    }

    /// Transitions the completed query into a paginated row stream.
    pub fn read(self) -> ReadRequest {
        ReadRequest::new(self)
    }

    pub fn query_metadata(&self) -> QueryMetadata {
        self.query_metadata.clone()
    }
}

pub struct ReadRequest {
    query: CompleteQuery,
    page_token: Option<String>,
    max_results: Option<u32>,
}

impl ReadRequest {
    fn new(query: CompleteQuery) -> Self {
        Self {
            query,
            page_token: None,
            max_results: None,
        }
    }

    pub fn with_page_token(mut self, page_token: impl Into<String>) -> Self {
        self.page_token = Some(page_token.into());
        self
    }

    pub fn with_max_results(mut self, max_results: u32) -> Self {
        self.max_results = Some(max_results);
        self
    }

    async fn execute(self) -> Result<RowIterator> {
        let schema = self.query.schema;
        let rows: VecDeque<Row> = self
            .query
            .cached_rows
            .into_iter()
            .map(|st| Row {
                values: wkt::Value::Object(st),
                schema: schema.clone(),
            })
            .collect();

        Ok(RowIterator {
            job_service: self.query.job_service,
            job_ref: self.query.job_ref,
            page_token: self.page_token.or(self.query.page_token),
            max_results: self.max_results,
            schema,
            rows,
        })
    }
}

impl std::future::IntoFuture for ReadRequest {
    type Output = crate::Result<crate::query::RowIterator>;
    type IntoFuture = futures::future::BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.execute())
    }
}
