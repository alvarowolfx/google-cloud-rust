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
use crate::query::{CompleteQuery, CompleteQueryJob, Row, RowIterator, Schema};
use google_cloud_bigquery_v2::client::JobService;
use std::collections::VecDeque;
use std::sync::Arc;

/// A request builder for configuring and initiating a paginated row stream.
pub struct ReadRequest {
    pub(crate) job_service: Arc<JobService>,
    pub(crate) job_ref: JobReference,
    pub(crate) cached_rows: VecDeque<wkt::Struct>,
    pub(crate) schema: Arc<Schema>,
    pub(crate) page_token: Option<String>,
    pub(crate) max_results: Option<u32>,
}

impl ReadRequest {
    /// Resumes reading from a specific page of results.
    pub fn with_page_token(mut self, page_token: impl Into<String>) -> Self {
        self.page_token = Some(page_token.into());
        self
    }

    /// Sets the maximum number of results to fetch per network page request.
    pub fn with_max_results(mut self, max_results: u32) -> Self {
        self.max_results = Some(max_results);
        self
    }

    async fn execute(self) -> Result<RowIterator> {
        let schema = self.schema;
        let rows: VecDeque<Row> = self
            .cached_rows
            .into_iter()
            .map(|st| Row::new(st, schema.clone()))
            .collect();

        Ok(RowIterator {
            job_service: self.job_service,
            job_ref: self.job_ref,
            page_token: self.page_token,
            max_results: self.max_results,
            schema,
            rows,
        })
    }
}

impl From<CompleteQuery> for ReadRequest {
    fn from(q: CompleteQuery) -> Self {
        Self {
            job_service: q.job_service,
            job_ref: q.job_ref,
            cached_rows: q.cached_rows,
            schema: q.schema,
            page_token: q.page_token,
            max_results: None,
        }
    }
}

impl From<CompleteQueryJob> for ReadRequest {
    fn from(q: CompleteQueryJob) -> Self {
        Self {
            job_service: q.job_service,
            job_ref: q.job_ref,
            cached_rows: q.cached_rows,
            schema: q.schema,
            page_token: q.page_token,
            max_results: None,
        }
    }
}

impl std::future::IntoFuture for ReadRequest {
    type Output = Result<RowIterator>;
    type IntoFuture = futures::future::BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.execute())
    }
}
