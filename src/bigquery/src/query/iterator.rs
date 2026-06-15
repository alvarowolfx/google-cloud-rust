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

use crate::error::{QueryError, RowError};
use crate::query::{CompleteQuery, Row, Schema};
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::{GetQueryResultsRequest, JobReference};
use std::collections::VecDeque;
use std::sync::Arc;

pub type Result<T> = std::result::Result<T, RowError>;
/// An iterator over rows returned by a query.
pub struct RowIterator {
    job_service: Arc<JobService>,
    job_ref: Option<JobReference>,
    schema: Arc<Schema>,
    page_token: Option<String>,
    max_results: Option<u32>,
    rows: VecDeque<wkt::Struct>,
    is_done: bool,
}

impl RowIterator {
    pub(crate) fn new(q: CompleteQuery) -> Self {
        let rows = q.cached_rows;
        let is_done = rows.is_empty() && q.page_token.is_none();

        Self {
            job_service: q.job_service,
            job_ref: q.job_ref,
            schema: q.schema,
            page_token: q.page_token,
            max_results: None,
            rows,
            is_done,
        }
    }

    /// Resumes reading from a specific page of results.
    pub fn with_page_token(mut self, page_token: impl Into<String>) -> Self {
        self.page_token = Some(page_token.into());
        self.rows.clear(); // Clear cached rows since they don't correspond to the custom page token.
        self.is_done = self.page_token.is_none();
        self
    }

    /// Sets the maximum number of results to fetch per network page request.
    pub fn with_max_results(mut self, max_results: u32) -> Self {
        self.max_results = Some(max_results);
        self
    }

    /// Fetches the next row from the result set.
    pub async fn next(&mut self) -> Option<Result<Row>> {
        if let Some(raw_row) = self.rows.pop_front() {
            let row = crate::query::row::convert_row(raw_row, &self.schema);
            return Some(row);
        }

        if self.is_done {
            return None;
        }

        if let Some(token) = self.page_token.take() {
            match self.fetch_page(token).await {
                Ok((fetched_rows, next_token)) => {
                    self.page_token = next_token;
                    self.rows.extend(fetched_rows);
                    if self.rows.is_empty() && self.page_token.is_none() {
                        self.is_done = true;
                        return None;
                    }

                    // Convert and return the first fetched row
                    self.rows
                        .pop_front()
                        .map(|r| crate::query::row::convert_row(r, &self.schema))
                }
                Err(e) => {
                    self.is_done = true;
                    Some(Err(e))
                }
            }
        } else {
            self.is_done = true;
            None
        }
    }

    async fn fetch_page(&self, token: String) -> Result<(Vec<wkt::Struct>, Option<String>)> {
        let Some(job_ref) = &self.job_ref else {
            unreachable!("This stateless queries should not return a page token")
        };

        let mut req = GetQueryResultsRequest::new()
            .set_project_id(job_ref.project_id.clone())
            .set_or_clear_max_results(self.max_results)
            .set_job_id(job_ref.job_id.clone())
            .set_page_token(token)
            .set_format_options(
                google_cloud_bigquery_v2::model::DataFormatOptions::new()
                    .set_use_int64_timestamp(true),
            );
        if let Some(location) = job_ref.location.clone() {
            req = req.set_location(location);
        }

        let res = self
            .job_service
            .get_query_results()
            .with_request(req)
            .send()
            .await
            .map_err(|e| RowError::Rpc { source: e })?;

        // TODO: Should we check this array?
        // At this point the job is done and we are just reading it.
        // if !res.errors.is_empty() {
        //   return Err(RowError::Rpc { source: e });
        // }

        let page_token = if res.page_token.is_empty() {
            None
        } else {
            Some(res.page_token)
        };

        Ok((res.rows, page_token))
    }
}
