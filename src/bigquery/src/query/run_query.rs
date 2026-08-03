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

use crate::error::QueryError;
use crate::model::RunQueryRequest;
use crate::query::execution::{InsertJobExecutor, PostQueryExecutor};
use crate::query::{Query, Result};
use crate::retry_policy::{
    JobRetryPolicy, JobRetryResult, RetryableJobErrors, default_job_retry_policy,
};
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::query_request::JobCreationMode;
use google_cloud_bigquery_v2::model::{
    InsertJobRequest, Job, JobConfiguration, JobReference, PostQueryRequest, QueryRequest,
};
use google_cloud_gax::retry_state::RetryState;
use std::sync::Arc;
use uuid::Uuid;

pub(crate) const JOB_ID_PREFIX: &str = "job_";
pub(crate) const QUERY_REQUEST_ID_PREFIX: &str = "";

/// A unified request builder for configuring and running a SQL query.
/// It automatically routes to either `jobs.query` (fast path) or `jobs.insert` (job path)
/// depending on the configured fields.
#[derive(Clone)]
pub struct RunQuery {
    pub(crate) job_service: Arc<JobService>,
    pub(crate) request: RunQueryRequest,
    pub(crate) project_id: Option<String>,
    pub(crate) job_retry_policy: Arc<dyn JobRetryPolicy>,
}

impl std::fmt::Debug for RunQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunQuery")
            .field("project_id", &self.project_id)
            .field("request_id", &self.request.request_id)
            .finish_non_exhaustive()
    }
}

impl RunQuery {
    /// Creates a new `RunQuery` builder for the given SQL query.
    pub(crate) fn new(job_service: Arc<JobService>, sql: String) -> Self {
        Self {
            job_service,
            request: RunQueryRequest::default()
                .set_query(sql)
                .set_use_legacy_sql(wkt::BoolValue::from(false))
                .set_job_creation_mode(JobCreationMode::JobCreationOptional),
            project_id: None,
            job_retry_policy: default_job_retry_policy(),
        }
    }

    /// Sets the project ID to override the default client project ID.
    pub fn with_project_id<S: Into<String>>(mut self, project_id: S) -> Self {
        self.project_id = Some(project_id.into());
        self
    }

    /// Sets the maximum number of attempts for reissuing the query when
    /// a retryable BigQuery server job error occurs.
    pub fn with_job_attempt_limit(mut self, attempt_limit: u32) -> Self {
        self.job_retry_policy =
            Arc::new(RetryableJobErrors::default().with_attempt_limit(attempt_limit));
        self
    }

    fn generate_job_reference(&self, project_id: &str) -> JobReference {
        let job_id = format!("{JOB_ID_PREFIX}{}", Uuid::new_v4());
        let mut job_ref = JobReference::new()
            .set_project_id(project_id.to_string())
            .set_job_id(job_id);

        if !self.request.location.is_empty() {
            job_ref = job_ref.set_location(self.request.location.clone());
        }

        job_ref
    }

    /// Executes the SQL query
    ///
    /// The implementation routes internally to [jobs.query] (fast path)
    /// or [jobs.insert] (job path) depending on configured fields.
    /// If the fast path is available, the client library takes it.
    /// If not, it falls back to creating a job, which is typically slower.
    ///
    /// [jobs.query]: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
    /// [jobs.insert]: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/insert
    pub async fn run(self) -> Result<Query> {
        let mut retry_state = RetryState::default();
        let job_retry_policy = self.job_retry_policy.clone();
        self.run_with_retry_state(&mut retry_state, &job_retry_policy)
            .await
    }

    pub(crate) async fn run_with_retry_state(
        &self,
        retry_state: &mut RetryState,
        job_retry_policy: &Arc<dyn JobRetryPolicy>,
    ) -> Result<Query> {
        let project_id = self
            .project_id
            .as_ref()
            .ok_or(QueryError::MissingProjectId)?;

        loop {
            match self.execute_once(project_id).await {
                Ok(mut query) => {
                    query.retry_state = retry_state.clone();
                    return Ok(query);
                }
                Err(err) => match job_retry_policy.on_error(retry_state, err) {
                    JobRetryResult::Continue(delay, _) => {
                        tokio::time::sleep(delay).await;
                        retry_state.attempt_count += 1;
                    }
                    JobRetryResult::Permanent(e) | JobRetryResult::Exhausted(e) => {
                        return Err(e);
                    }
                },
            }
        }
    }

    pub(crate) async fn execute_once(&self, project_id: &str) -> Result<Query> {
        if self.request.force_job_path() {
            // Route to jobs.insert
            let max_results = self.request.max_results;
            let job_config: JobConfiguration = self.request.clone().into();
            let job_ref = self.generate_job_reference(project_id);
            let job = Job::new()
                .set_configuration(job_config)
                .set_job_reference(job_ref);
            let req = InsertJobRequest::new()
                .set_job(job)
                .set_project_id(project_id.to_string());
            InsertJobExecutor::new(self.job_service.clone(), self.clone(), req, max_results)
                .execute()
                .await
        } else {
            let query_request_id = format!("{QUERY_REQUEST_ID_PREFIX}{}", Uuid::new_v4());
            // Route to jobs.query
            let query_request: QueryRequest = self.request.clone().into();
            let query_request = query_request
                .set_format_options(
                    google_cloud_bigquery_v2::model::DataFormatOptions::new()
                        .set_use_int64_timestamp(true),
                )
                .set_request_id(query_request_id);
            let req = PostQueryRequest::new()
                .set_project_id(project_id.to_string())
                .set_query_request(query_request);

            PostQueryExecutor::new(self.job_service.clone(), self.clone(), req)
                .execute()
                .await
        }
    }
}

include!("../generated/run_query_builder.rs");

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::tests::{MockJobService, create_job_service};
    use google_cloud_bigquery_v2::model::query_request::JobCreationMode;
    use google_cloud_bigquery_v2::model::{
        ErrorProto, Job, JobConfiguration, JobReference, JobStatus, QueryRequest, QueryResponse,
    };
    use google_cloud_gax::response::Response;

    type TestResult = anyhow::Result<()>;

    #[test]
    fn test_new() {
        let job_service = create_job_service(MockJobService::new());
        let sql = "SELECT 1".to_string();
        let run_query = RunQuery::new(job_service, sql.clone());
        assert_eq!(run_query.request.query, sql);
        assert_eq!(
            run_query.request.use_legacy_sql,
            Some(wkt::BoolValue::from(false))
        );
        assert_eq!(
            run_query.request.job_creation_mode,
            JobCreationMode::JobCreationOptional
        );
        assert_eq!(run_query.project_id, None);
    }

    #[test]
    fn test_with_project_id() {
        let job_service = create_job_service(MockJobService::new());
        let run_query =
            RunQuery::new(job_service, "SELECT 1".to_string()).with_project_id("my-project");
        assert_eq!(run_query.project_id.unwrap(), "my-project");
    }

    #[tokio::test]
    async fn test_run_missing_project_id() {
        let job_service = create_job_service(MockJobService::new());
        let run_query = RunQuery::new(job_service, "SELECT 1".to_string());
        let res = run_query.run().await;
        assert!(matches!(res, Err(QueryError::MissingProjectId)));
    }

    #[tokio::test]
    async fn test_run_jobs_insert() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_insert_job().returning(|_, _| {
            let job_ref = JobReference::new()
                .set_job_id("test-job")
                .set_project_id("my-project");
            let job = Job::new()
                .set_job_reference(job_ref)
                .set_status(JobStatus::new().set_state("DONE"));
            Ok(google_cloud_gax::response::Response::from(job))
        });
        let job_service = create_job_service(mock);

        let run_query = RunQuery::new(job_service, "SELECT 1".to_string())
            .with_project_id("my-project")
            .set_allow_large_results(true);
        let query = run_query.run().await?;
        assert!(query.completed, "{query:?}");

        Ok(())
    }

    #[tokio::test]
    async fn test_run_jobs_query() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_query()
            .returning(move |_, _| Ok(Response::from(QueryResponse::new())));
        let job_service = create_job_service(mock);
        let run_query =
            RunQuery::new(job_service, "SELECT 1".to_string()).with_project_id("my-project");
        let query = run_query.run().await?;
        assert!(!query.completed, "{query:?}");
        assert_eq!(query.metadata().query_id, "");

        Ok(())
    }

    #[test]
    fn test_force_job_path() {
        let job_service = create_job_service(MockJobService::new());
        let mut run_query = RunQuery::new(job_service, "SELECT 1".to_string());
        assert!(!run_query.request.force_job_path());

        // setting a jobs.insert exclusive field
        run_query = run_query.set_allow_large_results(true);
        assert!(run_query.request.force_job_path());
    }

    #[test]
    fn test_request_conversions() {
        let req = RunQueryRequest::default()
            .set_query("SELECT 1".to_string())
            .set_dry_run(true)
            .set_use_legacy_sql(true);

        let query_request: QueryRequest = req.clone().into();
        assert_eq!(query_request.query, "SELECT 1");
        assert!(query_request.dry_run);
        assert_eq!(
            query_request.use_legacy_sql,
            Some(wkt::BoolValue::from(true))
        );

        let job_config: JobConfiguration = req.into();
        let job_query = job_config.query.as_ref().unwrap();
        assert_eq!(job_query.query, "SELECT 1");
        assert_eq!(job_query.use_legacy_sql, Some(wkt::BoolValue::from(true)));
    }

    #[test]
    fn test_id_generation() {
        let job_service = create_job_service(MockJobService::new());
        let run_query = RunQuery::new(job_service.clone(), "SELECT 1".to_string());

        let job_ref = run_query.generate_job_reference("my-project");
        assert_eq!(job_ref.project_id, "my-project");
        assert!(job_ref.job_id.starts_with(JOB_ID_PREFIX));
    }

    #[tokio::test(start_paused = true)]
    async fn test_run_reissue_on_retryable_job_failed() -> TestResult {
        let mut mock = MockJobService::new();
        let mut seq = mockall::Sequence::new();
        let first_request_id = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
        let first_request_id_clone = first_request_id.clone();

        mock.expect_query()
            .in_sequence(&mut seq)
            .times(1)
            .returning(move |req, _| {
                let req_id = req.query_request.as_ref().unwrap().request_id.clone();
                assert!(req_id.starts_with(QUERY_REQUEST_ID_PREFIX));
                assert!(uuid::Uuid::parse_str(&req_id[QUERY_REQUEST_ID_PREFIX.len()..]).is_ok());
                *first_request_id_clone.lock().unwrap() = req_id;
                let err_proto = ErrorProto::new()
                    .set_reason("backendError")
                    .set_message("temporary server issue");
                Ok(Response::from(
                    QueryResponse::new().set_errors(vec![err_proto]),
                ))
            });

        mock.expect_query()
            .in_sequence(&mut seq)
            .times(1)
            .returning(move |req, _| {
                let req_id = req.query_request.as_ref().unwrap().request_id.clone();
                assert!(req_id.starts_with(QUERY_REQUEST_ID_PREFIX));
                assert!(uuid::Uuid::parse_str(&req_id[QUERY_REQUEST_ID_PREFIX.len()..]).is_ok());
                assert_ne!(
                    req_id,
                    *first_request_id.lock().unwrap(),
                    "reissued query must generate a fresh request_id to avoid 409 duplicate ID conflicts"
                );
                Ok(Response::from(
                    QueryResponse::new().set_query_id("q_success"),
                ))
            });

        let job_service = create_job_service(mock);
        let run_query =
            RunQuery::new(job_service, "SELECT 1".to_string()).with_project_id("my-project");
        let query = run_query.run().await?;
        assert_eq!(query.metadata().query_id, "q_success");

        Ok(())
    }

    #[tokio::test]
    async fn test_run_jobs_query_with_max_results() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_query().returning(move |req, _| {
            assert_eq!(
                req.query_request.as_ref().and_then(|r| r.max_results),
                Some(100)
            );
            Ok(Response::from(QueryResponse::new()))
        });
        let job_service = create_job_service(mock);
        let run_query = RunQuery::new(job_service, "SELECT 1".to_string())
            .with_project_id("my-project")
            .set_max_results(100_u32);
        let query = run_query.run().await?;
        assert_eq!(query.max_results, Some(100));

        Ok(())
    }

    #[tokio::test]
    async fn test_run_jobs_insert_with_max_results() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_insert_job().returning(|_, _| {
            let job_ref = JobReference::new()
                .set_job_id("test-job")
                .set_project_id("my-project");
            let job = Job::new()
                .set_job_reference(job_ref)
                .set_status(JobStatus::new().set_state("DONE"));
            Ok(google_cloud_gax::response::Response::from(job))
        });
        let job_service = create_job_service(mock);

        let run_query = RunQuery::new(job_service, "SELECT 1".to_string())
            .with_project_id("my-project")
            .set_allow_large_results(true)
            .set_max_results(50_u32);
        let query = run_query.run().await?;
        assert_eq!(query.max_results, Some(50));

        Ok(())
    }
}
