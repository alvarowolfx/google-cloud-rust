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

pub(crate) mod execution;
pub(crate) mod from_sql;
mod iterator;
mod query_handle;
mod query_reference;
mod row;
mod run_query;
mod schema;

/// Result type for query execution.
pub type Result<T> = std::result::Result<T, crate::error::QueryError>;

pub use from_sql::{FromSql, Interval, Range, deserialize};
pub use iterator::RowIterator;
pub use query_handle::{CompleteQuery, Query};
pub use query_reference::QueryReference;
pub use row::Row;
pub use run_query::RunQuery;
pub(crate) use schema::Schema;

#[cfg(test)]
pub(crate) mod tests {
    use crate::retry_policy::{JobRetryPolicy, RetryableJobErrors};
    use google_cloud_bigquery_v2::Result;
    use google_cloud_bigquery_v2::client::JobService;
    use google_cloud_bigquery_v2::model::{
        GetJobRequest, GetQueryResultsRequest, GetQueryResultsResponse, InsertJobRequest, Job,
        PostQueryRequest, QueryResponse,
    };
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_gax::polling_backoff_policy::PollingBackoffPolicy;
    use google_cloud_gax::polling_state::PollingState;
    use google_cloud_gax::response::Response;
    use google_cloud_gax::retry_state::RetryState;
    use std::sync::Arc;

    mockall::mock! {
        #[derive(Debug)]
        pub JobService {}
        impl google_cloud_bigquery_v2::stub::JobService for JobService {
            async fn get_job(
                &self,
                req: GetJobRequest,
                options: RequestOptions,
            ) -> Result<Response<Job>>;
            async fn insert_job(
                &self,
                req: InsertJobRequest,
                options: RequestOptions,
            ) -> Result<Response<Job>>;
            async fn query(
                &self,
                req: PostQueryRequest,
                options: RequestOptions,
            ) -> Result<Response<QueryResponse>>;
            async fn get_query_results(
                &self,
                req: GetQueryResultsRequest,
                options: RequestOptions,
            ) -> Result<Response<GetQueryResultsResponse>>;
        }
    }

    mockall::mock! {
        #[derive(Debug)]
        pub BackoffPolicy {}
        impl PollingBackoffPolicy for BackoffPolicy {
            fn wait_period(&self, _state: &PollingState) -> std::time::Duration;
        }
        impl google_cloud_gax::backoff_policy::BackoffPolicy for BackoffPolicy {
            fn on_failure(&self, state: &RetryState) -> std::time::Duration;
        }
    }

    pub(crate) fn create_job_service(mock: MockJobService) -> Arc<JobService> {
        Arc::new(JobService::from_stub::<MockJobService>(Arc::new(mock)))
    }

    pub(crate) fn create_test_backoff_policy() -> MockBackoffPolicy {
        MockBackoffPolicy::new()
    }

    pub(crate) fn create_test_job_retry_policy() -> Arc<dyn JobRetryPolicy> {
        let mut backoff = MockBackoffPolicy::new();
        backoff
            .expect_on_failure()
            .return_const(std::time::Duration::ZERO);
        Arc::new(RetryableJobErrors::default().with_backoff_policy(backoff))
    }
}
