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
    pub(crate) project_id: String,
    pub(crate) job_id: String,
    pub(crate) location: String,
    pub(crate) page_token: String,
    pub(crate) schema: Option<Arc<Schema>>,
    pub(crate) rows: VecDeque<Row>,
}

impl RowIterator {
    /// Returns the next row in the result set.
    /// Automatically fetches the next page from the network if the buffer is empty.
    pub async fn next(&mut self) -> Option<Result<Row>> {
        if let Some(row) = self.rows.pop_front() {
            return Some(Ok(row));
        }

        if self.page_token.is_empty() {
            return None;
        }

        let mut req = GetQueryResultsRequest::new()
            .set_project_id(self.project_id.clone())
            .set_job_id(self.job_id.clone())
            .set_page_token(self.page_token.clone());
        if !self.location.is_empty() {
            req = req.set_location(self.location.clone());
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

        self.page_token = res.page_token;

        let schema = if let Some(ref s) = self.schema {
            s.clone()
        } else {
            return Some(Err(google_cloud_gax::error::Error::io(
                Error::MissingSchema,
            )));
        };

        self.rows = res
            .rows
            .into_iter()
            .map(|st| Row::new(st, schema.clone()))
            .collect();

        self.rows.pop_front().map(Ok)
    }
}
