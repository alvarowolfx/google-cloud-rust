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
use crate::query::execution::{InsertJobExecutor, PostQueryExecutor};
use crate::query::query_handle::Query;
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::{
    Job, JobConfiguration, JobConfigurationQuery, PostQueryRequest, QueryRequest,
};
use std::sync::Arc;

/// A unified request builder for configuring and running a SQL query.
/// It automatically routes to either `jobs.query` (fast path) or `jobs.insert` (job path)
/// depending on the configured fields.
#[derive(Clone, Debug)]
pub struct RunQuery {
    pub(crate) job_service: Arc<JobService>,
    pub(crate) query_request: QueryRequest,
    pub(crate) job_config: JobConfiguration,
    pub(crate) force_job_path: bool,
    pub(crate) project_id: Option<String>,
}

impl RunQuery {
    /// Creates a new `RunQuery` builder for the given SQL query.
    pub fn new(job_service: Arc<JobService>, sql: String) -> Self {
        Self {
            job_service,
            query_request: QueryRequest::new()
                .set_query(sql.clone())
                .set_use_legacy_sql(wkt::BoolValue::from(false)),
            job_config: JobConfiguration::new().set_query(
                JobConfigurationQuery::new()
                    .set_query(sql)
                    .set_use_legacy_sql(wkt::BoolValue::from(false)),
            ),
            force_job_path: false,
            project_id: None,
        }
    }

    /// Sets the project ID to override the default client project ID.
    pub fn with_project_id<S: Into<String>>(mut self, project_id: S) -> Self {
        self.project_id = Some(project_id.into());
        self
    }

    /// Executes the SQL query, routing internally to `jobs.query` (fast path)
    /// or `jobs.insert` (job path) depending on configured fields.
    pub async fn run(self) -> Result<Query> {
        let project_id = self
            .project_id
            .clone()
            .ok_or_else(|| google_cloud_gax::error::Error::io("No project id provided"))?;

        if self.force_job_path {
            // Route to jobs.insert (Job Path) using InsertJobExecutor
            let job = Job::new().set_configuration(self.job_config);

            InsertJobExecutor {
                job_service: self.job_service,
                job,
                project_id,
            }
            .execute()
            .await
        } else {
            // Route to jobs.query (Fast Path) using PostQueryExecutor
            let req = PostQueryRequest::new()
                .set_project_id(project_id)
                .set_query_request(self.query_request);

            PostQueryExecutor {
                job_service: self.job_service,
                request: req,
            }
            .execute()
            .await
        }
    }
}

include!("../generated/run_query_builder.rs");
