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

use crate::query::{CompleteQuery, QueryMetadata, ResultsPage, Row, Schema};
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::{GetQueryResultsRequest, JobReference};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

/// A request builder for configuring and initiating a paginated row stream.
pub struct ReadRequest {
    pub(crate) job_service: Arc<JobService>,
    pub(crate) job_ref: Option<JobReference>,
    pub(crate) cached_rows: VecDeque<wkt::Struct>,
    pub(crate) schema: Arc<Schema>,
    pub(crate) page_token: Option<String>,
    pub(crate) max_results: Option<u32>,
    pub(crate) metadata: QueryMetadata,
}

impl ReadRequest {
    pub(crate) fn new(q: CompleteQuery) -> Self {
        Self {
            job_service: q.job_service,
            job_ref: q.job_ref,
            cached_rows: q.cached_rows,
            schema: q.schema,
            page_token: q.page_token,
            max_results: None,
            metadata: q.metadata,
        }
    }

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

    /// Streams each page in the collection.
    pub fn by_page(
        self,
    ) -> impl google_cloud_gax::paginator::Paginator<ResultsPage, google_cloud_gax::error::Error>
    {
        let first_page = make_first_page(
            self.cached_rows,
            self.schema.clone(),
            self.page_token.clone(),
            self.metadata.clone(),
        );

        let first_page_cell = Arc::new(Mutex::new(Some(first_page)));

        let job_service = self.job_service;
        let job_ref = self.job_ref;
        let max_results = self.max_results;
        let schema = self.schema;

        let execute = move |token: String| {
            let first_page_cell = first_page_cell.clone();
            let job_service = job_service.clone();
            let job_ref = job_ref.clone();
            let schema = schema.clone();

            async move {
                // Try to take the pre-cached first page
                let first_page = {
                    let mut guard = first_page_cell.lock().unwrap();
                    guard.take()
                }; // Lock is explicitly dropped here

                if let Some(page) = first_page {
                    return Ok(page);
                }

                // Otherwise, fetch subsequent pages from the network
                fetch_page(job_service, job_ref, max_results, schema, token).await
            }
        };

        // Seed with a dummy token. The first execute("") call is intercepted
        // and returns the pre-cached first page.
        google_cloud_gax::paginator::internal::new_paginator(String::new(), execute)
    }

    /// Streams each row in the collection.
    pub fn by_row(
        self,
    ) -> impl google_cloud_gax::paginator::ItemPaginator<ResultsPage, google_cloud_gax::error::Error>
    {
        use google_cloud_gax::paginator::Paginator;
        self.by_page().items()
    }
}

fn make_first_page(
    cached_rows: VecDeque<wkt::Struct>,
    schema: Arc<Schema>,
    page_token: Option<String>,
    metadata: QueryMetadata,
) -> ResultsPage {
    ResultsPage {
        rows: cached_rows
            .into_iter()
            .map(|st| Row::new(st, schema.clone()))
            .collect(),
        page_token,
        metadata,
    }
}

async fn fetch_page(
    job_service: Arc<JobService>,
    job_ref: Option<JobReference>,
    max_results: Option<u32>,
    schema: Arc<Schema>,
    token: String,
) -> Result<ResultsPage, google_cloud_gax::error::Error> {
    let Some(job_ref) = job_ref else {
        return Err(google_cloud_gax::error::Error::io(
            "Stateless queries can't have more pages",
        ));
    };

    let mut req = GetQueryResultsRequest::new()
        .set_project_id(job_ref.project_id.clone())
        .set_or_clear_max_results(max_results)
        .set_job_id(job_ref.job_id.clone())
        .set_page_token(token);
    if let Some(location) = job_ref.location.clone() {
        req = req.set_location(location);
    }

    let res = job_service
        .get_query_results()
        .with_request(req)
        .send()
        .await?;

    if let Some(first_err) = res.errors.first() {
        let rpc_status = google_cloud_gax::error::rpc::Status::default()
            .set_code(google_cloud_gax::error::rpc::Code::Unknown)
            .set_message(first_err.message.clone());
        return Err(google_cloud_gax::error::Error::service(rpc_status));
    }

    let metadata = QueryMetadata::GetQueryResultsResponse(res.clone());

    let page_token = if res.page_token.is_empty() {
        None
    } else {
        Some(res.page_token)
    };

    let rows = res
        .rows
        .into_iter()
        .map(|st| Row::new(st, schema.clone()))
        .collect();

    Ok(ResultsPage {
        rows,
        page_token,
        metadata,
    })
}
