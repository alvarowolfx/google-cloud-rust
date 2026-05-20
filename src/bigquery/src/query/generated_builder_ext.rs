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
use crate::query::run_query::RunQuery;
use google_cloud_bigquery_v2::model::{Job, PostQueryRequest};

impl RunQuery {
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
