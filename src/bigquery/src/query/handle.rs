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
use crate::query::{IteratorError, Row, RowIterator, Schema};
use google_cloud_bigquery_v2::client::JobService;
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
    pub(crate) project_id: String,
    pub(crate) job_id: String,
    pub(crate) location: String,
    pub(crate) completed: bool,
    pub(crate) total_rows: u64,
    pub(crate) num_dml_affected_rows: i64,
    pub(crate) cached_rows: VecDeque<wkt::Struct>,
    pub(crate) schema: Option<Arc<Schema>>,
    pub(crate) page_token: String,
}

impl Query {
    /// Periodically checks the status of the background job until it finishes.
    /// Returns an error if a remote service or connection failure happens during polling.
    pub async fn wait(&mut self) -> Result<()> {
        if self.completed {
            return Ok(());
        }

        let mut delay = std::time::Duration::from_millis(500);
        let max_delay = std::time::Duration::from_secs(10);

        loop {
            let mut req = google_cloud_bigquery_v2::model::GetQueryResultsRequest::new()
                .set_project_id(self.project_id.clone())
                .set_job_id(self.job_id.clone());
            if !self.location.is_empty() {
                req = req.set_location(self.location.clone());
            }

            let res = self
                .job_service
                .get_query_results()
                .with_request(req)
                .send()
                .await?;

            if let Some(first_err) = res.errors.into_iter().next() {
                let rpc_status = google_cloud_gax::error::rpc::Status::default()
                    .set_code(google_cloud_gax::error::rpc::Code::Unknown)
                    .set_message(first_err.message);
                return Err(google_cloud_gax::error::Error::service(rpc_status));
            }

            if res.job_complete.unwrap_or(false) {
                self.completed = true;
                self.total_rows = res.total_rows.unwrap_or(0);
                self.num_dml_affected_rows = res.num_dml_affected_rows.unwrap_or(0);
                if let Some(s) = res.schema {
                    self.schema = Some(Arc::new(s));
                }
                self.cached_rows = VecDeque::from(res.rows);
                self.page_token = res.page_token;
                return Ok(());
            }

            tokio::time::sleep(delay).await;
            delay = std::cmp::min(delay * 2, max_delay);
        }
    }

    /// Transitions the completed query into a paginated row stream.
    /// Returns `Err(Error::NotComplete)` if called before the background job has finished.
    pub async fn read(self) -> Result<RowIterator> {
        if !self.completed {
            return Err(google_cloud_gax::error::Error::io(Error::NotComplete));
        }

        let schema_arc = if let Some(s) = self.schema {
            s
        } else {
            return Err(google_cloud_gax::error::Error::io(
                IteratorError::MissingSchema,
            ));
        };

        let rows: VecDeque<Row> = self
            .cached_rows
            .into_iter()
            .map(|st| Row {
                values: wkt::Value::Object(st),
                schema: schema_arc.clone(),
            })
            .collect();

        Ok(RowIterator {
            job_service: self.job_service,
            project_id: self.project_id,
            job_id: self.job_id,
            location: self.location,
            page_token: self.page_token,
            schema: Some(schema_arc),
            rows,
        })
    }

    /// Returns the underlying job reference for this query.
    pub fn job_reference(&self) -> Option<google_cloud_bigquery_v2::model::JobReference> {
        let mut jr = google_cloud_bigquery_v2::model::JobReference::new()
            .set_project_id(self.project_id.clone())
            .set_job_id(self.job_id.clone());
        if !self.location.is_empty() {
            jr = jr.set_location(self.location.clone());
        }
        Some(jr)
    }

    /// Returns whether the query job is complete.
    pub fn completed(&self) -> bool {
        self.completed
    }

    /// Returns the total number of rows in the complete query result set.
    pub fn total_rows(&self) -> u64 {
        self.total_rows
    }

    /// Returns the number of rows affected by a DML statement.
    pub fn num_dml_affected_rows(&self) -> i64 {
        self.num_dml_affected_rows
    }
}
