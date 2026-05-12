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
use crate::query::{Row, Schema};
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::GetQueryResultsRequest;
use std::collections::VecDeque;
use std::sync::Arc;

/// Represents errors that can occur when reading query results.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Only complete Query Jobs with schema can be read.
    #[error("Only complete Query Jobs with schema can be read.")]
    MissingSchema,
}

/// A paginated stream over the rows of a query result.
pub struct RowIterator {
    pub(crate) job_service: Arc<JobService>,
    pub(crate) job_ref: JobReference,
    pub(crate) page_token: Option<String>,
    pub(crate) schema: Arc<Schema>,
    pub(crate) rows: VecDeque<Row>,
}

impl RowIterator {
    /// Returns the next row in the result set.
    /// Automatically fetches the next page from the network if the buffer is empty.
    pub async fn next(&mut self) -> Option<Result<Row>> {
        if let Some(row) = self.rows.pop_front() {
            return Some(Ok(row));
        }

        let Some(page_token) = &self.page_token else {
            return None;
        };

        let Some(job_ref) = self.job_ref.as_job_ref() else {
            return Some(Err(google_cloud_gax::error::Error::io(
                "Stateless queries can't have more pages",
            )));
        };

        let mut req = GetQueryResultsRequest::new()
            .set_project_id(job_ref.project_id.clone())
            .set_job_id(job_ref.job_id.clone())
            .set_page_token(page_token);
        if let Some(location) = job_ref.location.clone() {
            req = req.set_location(location);
        }

        let res = match self
            .job_service
            .get_query_results()
            .with_request(req)
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => return Some(Err(e)),
        };

        if let Some(first_err) = res.errors.into_iter().next() {
            let rpc_status = google_cloud_gax::error::rpc::Status::default()
                .set_code(google_cloud_gax::error::rpc::Code::Unknown)
                .set_message(first_err.message);
            return Some(Err(google_cloud_gax::error::Error::service(rpc_status)));
        }

        self.page_token = if res.page_token.is_empty() {
            None
        } else {
            Some(res.page_token)
        };

        self.rows = res
            .rows
            .into_iter()
            .map(|st| Row::new(st, self.schema.clone()))
            .collect();

        self.rows.pop_front().map(Ok)
    }
}
