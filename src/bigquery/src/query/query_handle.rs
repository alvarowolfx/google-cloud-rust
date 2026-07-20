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
use crate::model::{QueryMetadata, RunQueryRequest};
use crate::query::{QueryReference, Result, RowIterator, RunQuery, Schema};
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::{
    GetQueryResultsRequest, GetQueryResultsResponse, Job, JobReference, QueryResponse,
};
use google_cloud_gax::backoff_policy::BackoffPolicy;
use google_cloud_gax::error::Error as GaxError;
use google_cloud_gax::error::rpc::{Code, Status};
use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
use google_cloud_gax::options::RequestOptionsBuilder;
use google_cloud_gax::polling_backoff_policy::PollingBackoffPolicy;
use google_cloud_gax::polling_state::PollingState;
use google_cloud_gax::retry_policy::RetryPolicy;
use google_cloud_gax::retry_state::RetryState;
use std::collections::VecDeque;
use std::sync::Arc;

/// A handle representing a running query.
#[derive(Clone, Debug)]
pub struct Query {
    pub(crate) job_service: Arc<JobService>,
    pub(crate) job_ref: Option<JobReference>,
    pub(crate) completed: bool,
    pub(crate) initial_response: Option<QueryResponse>,
    pub(crate) initial_job: Option<Job>,
    pub(crate) query_template: Option<RunQuery>,
    pub(crate) retry_state: RetryState,
    pub(crate) retry_policy: Arc<dyn RetryPolicy>,
    pub(crate) backoff_policy: Arc<dyn BackoffPolicy>,
}

impl Query {
    /// Returns the [`QueryReference`] for this query.
    ///
    /// The reference will be [`QueryReference::Job`] with a query [job reference],
    /// or [`QueryReference::Stateless`] with an opaque query ID if job creation
    /// was skipped.
    ///
    /// [job reference]: https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/JobReference
    pub fn query_reference(&self) -> QueryReference {
        let from_query_id = self
            .initial_response
            .as_ref()
            .map(|res| res.query_id.clone())
            .filter(|s| !s.is_empty())
            .map(QueryReference::from_query_id);
        let from_job_ref = self.job_ref.clone().map(QueryReference::from);

        from_job_ref
            .or(from_query_id)
            .expect("query must have either a job reference or query id")
    }

    /// Periodically checks the status of the background job until it finishes.
    /// Returns an error if a remote service or connection failure happens during polling.
    pub async fn until_done(mut self) -> Result<CompleteQuery> {
        let mut retry_state = self.retry_state.clone();
        let retry_policy = self.retry_policy.clone();
        let backoff_policy = self.backoff_policy.clone();

        loop {
            if let (true, Some(initial_response)) = (self.completed, self.initial_response.take()) {
                return Ok(CompleteQuery::from_query_response(
                    self.job_service.clone(),
                    self.job_ref.clone(),
                    initial_response,
                    retry_policy.clone(),
                    backoff_policy.clone(),
                ));
            }

            let job_ref = self
                .job_ref
                .as_ref()
                .expect("query job should have job reference at this point")
                .clone();
            let polling_backoff_policy = Arc::new(
                ExponentialBackoffBuilder::default()
                    .with_initial_delay(std::time::Duration::from_secs(10))
                    .build()
                    .expect("valid backoff configuration"),
            );

            match poll_query_results(
                &self.job_service,
                &job_ref,
                polling_backoff_policy,
                retry_policy.clone(),
                backoff_policy.clone(),
            )
            .await
            {
                Ok(res) => {
                    self.retry_state = retry_state;
                    return Ok(CompleteQuery::from_get_query_results_response(
                        self.job_service.clone(),
                        &job_ref,
                        res,
                        retry_policy.clone(),
                        backoff_policy.clone(),
                    ));
                }
                Err(err) if crate::retry_policy::is_query_error_retryable(&err) => {
                    if let Some(gax_err) = crate::retry_policy::query_job_failed_to_gax_error(&err)
                        && retry_policy.on_error(&retry_state, gax_err).is_continue()
                        && let Some(query_template) = self.query_template.clone()
                    {
                        let delay = backoff_policy.on_failure(&retry_state);
                        tokio::time::sleep(delay).await;
                        retry_state.attempt_count += 1;

                        self.retry_state = retry_state.clone();
                        let new_query = query_template
                            .run_with_retry_state(&mut retry_state, &retry_policy, &backoff_policy)
                            .await?;
                        self.job_ref = new_query.job_ref;
                        self.completed = new_query.completed;
                        self.initial_response = new_query.initial_response;
                        self.initial_job = new_query.initial_job;
                        self.retry_state = new_query.retry_state;
                        continue;
                    }
                    return Err(err);
                }
                Err(err) => return Err(err),
            }
        }
    }
}

/// A handle representing a successfully completed query ready for reading.
#[derive(Clone)]
pub struct CompleteQuery {
    pub(crate) job_service: Arc<JobService>,
    pub(crate) job_ref: Option<JobReference>,
    pub(crate) cached_rows: VecDeque<wkt::Struct>,
    pub(crate) schema: Arc<Schema>,
    pub(crate) page_token: Option<String>,
    pub(crate) metadata: QueryMetadata,
    pub(crate) retry_policy: Arc<dyn RetryPolicy>,
    pub(crate) backoff_policy: Arc<dyn BackoffPolicy>,
}

impl std::fmt::Debug for CompleteQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompleteQuery")
            .field("job_ref", &self.job_ref)
            .field("cached_rows", &self.cached_rows)
            .field("schema", &self.schema)
            .field("page_token", &self.page_token)
            .finish_non_exhaustive()
    }
}

impl CompleteQuery {
    pub(crate) fn from_get_query_results_response(
        job_service: Arc<JobService>,
        job_ref: &JobReference,
        mut res: GetQueryResultsResponse,
        retry_policy: Arc<dyn RetryPolicy>,
        backoff_policy: Arc<dyn BackoffPolicy>,
    ) -> Self {
        let cached_rows = VecDeque::from(std::mem::take(&mut res.rows));
        let metadata = QueryMetadata::from(res);
        let schema = metadata
            .schema
            .clone()
            .expect("complete query should have schema");
        let schema = Arc::new(Schema::new(schema));
        let page_token = if metadata.page_token.is_empty() {
            None
        } else {
            Some(metadata.page_token.clone())
        };
        Self {
            job_service,
            job_ref: Some(job_ref.clone()),
            cached_rows,
            page_token,
            schema,
            metadata,
            retry_policy,
            backoff_policy,
        }
    }

    pub(crate) fn from_query_response(
        job_service: Arc<JobService>,
        job_ref: Option<JobReference>,
        mut res: QueryResponse,
        retry_policy: Arc<dyn RetryPolicy>,
        backoff_policy: Arc<dyn BackoffPolicy>,
    ) -> Self {
        let cached_rows = VecDeque::from(std::mem::take(&mut res.rows));
        let metadata = QueryMetadata::from(res);
        let schema = metadata
            .schema
            .clone()
            .expect("complete query should have schema");
        let schema = Arc::new(Schema::new(schema));
        let page_token = if metadata.page_token.is_empty() {
            None
        } else {
            Some(metadata.page_token.clone())
        };
        Self {
            job_service,
            job_ref,
            cached_rows,
            page_token,
            schema,
            metadata,
            retry_policy,
            backoff_policy,
        }
    }

    /// Returns a row iterator for the query result.
    pub fn read(self) -> RowIterator {
        RowIterator::new(self)
    }

    /// Returns the cached metadata for this query.
    pub fn metadata(&self) -> &QueryMetadata {
        &self.metadata
    }

    /// Performs a network call to fetch the full `Job` resource from the backend.
    pub async fn job_metadata(&self) -> Result<Job> {
        let job_ref = self.job_ref.as_ref().ok_or(QueryError::StatelessQuery)?;

        let mut req = self
            .job_service
            .get_job()
            .set_job_id(job_ref.job_id.clone())
            .set_project_id(job_ref.project_id.clone())
            .with_retry_policy(self.retry_policy.clone())
            .with_backoff_policy(self.backoff_policy.clone());

        if let Some(location) = job_ref.location.clone() {
            req = req.set_location(location);
        }

        let job = req
            .send()
            .await
            .map_err(|e| QueryError::Rpc { source: e })?;
        Ok(job)
    }
}

/// Helper function to poll getQueryResults until a job finishes.
pub(crate) async fn poll_query_results(
    job_service: &JobService,
    job_ref: &JobReference,
    polling_backoff_policy: Arc<dyn PollingBackoffPolicy>,
    retry_policy: Arc<dyn RetryPolicy>,
    backoff_policy: Arc<dyn BackoffPolicy>,
) -> Result<GetQueryResultsResponse> {
    let mut state = PollingState::default();

    loop {
        let mut req = GetQueryResultsRequest::new()
            .set_max_results(0u32)
            .set_project_id(job_ref.project_id.clone())
            .set_job_id(job_ref.job_id.clone());
        if let Some(location) = job_ref.location.clone() {
            req = req.set_location(location);
        }

        let res = job_service
            .get_query_results()
            .with_request(req)
            .with_retry_policy(retry_policy.clone())
            .with_backoff_policy(backoff_policy.clone())
            .send()
            .await?;

        if !res.errors.is_empty() {
            return Err(QueryError::JobFailed { errors: res.errors });
        }

        let completed = res.job_complete.unwrap_or(false);
        if completed {
            return Ok(res);
        }

        let delay = polling_backoff_policy.wait_period(&state);
        tokio::time::sleep(delay).await;
        // TODO(#5592): limit retry attempts or add cancellation mechanism
        state.attempt_count += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::RunQuery;
    use crate::query::run_query::JOB_ID_PREFIX;
    use crate::query::tests::{
        MockBackoffPolicy, MockJobService, create_job_service, create_test_backoff_policy,
        create_test_retry_backoff_policy, create_test_retry_policy,
    };
    use crate::retry_policy::RetryableErrors;
    use google_cloud_bigquery_v2::model::{
        ErrorProto, GetQueryResultsResponse, JobReference, QueryResponse, TableFieldSchema,
        TableSchema,
    };
    use google_cloud_gax::error::Error as GaxError;
    use google_cloud_gax::error::rpc::{Code, Status};
    use google_cloud_gax::response::Response;
    use google_cloud_gax::retry_policy::RetryPolicyExt;
    use std::time::Duration;
    use test_case::test_case;

    type TestResult = anyhow::Result<()>;

    #[test_case(Some("query_123"), None, QueryReference::Stateless{ query_id: "query_123".to_string()}; "with query id")]
    #[test_case(Some(""), Some(JobReference::new()), QueryReference::Job(JobReference::new()); "empty query id")]
    #[test_case(None, Some(JobReference::new()), QueryReference::Job(JobReference::new()); "with job refearence")]
    #[test_case(Some("query_123"), Some(JobReference::new()), QueryReference::Job(JobReference::new()); "with both job reference and query id")]
    fn test_query_query_reference(
        query_id: Option<&str>,
        job_ref: Option<JobReference>,
        expected: QueryReference,
    ) {
        let job_service = create_job_service(MockJobService::new());
        let initial_response = query_id.map(|id| QueryResponse::new().set_query_id(id));

        let query = Query {
            job_service: job_service.clone(),
            job_ref,
            completed: false,
            initial_job: None,
            initial_response,
            query_template: Some(RunQuery::new(job_service.clone(), "SELECT 1".to_string())),
            retry_policy: Arc::new(create_test_retry_policy()),
            backoff_policy: Arc::new(create_test_retry_backoff_policy()),
            retry_state: RetryState::new(true),
        };

        let result = query.query_reference();
        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn test_query_until_done_already_completed() -> TestResult {
        let job_service = create_job_service(MockJobService::new());
        let job_ref = JobReference::new()
            .set_project_id("some_project")
            .set_job_id("some_job_id");
        let query_res = QueryResponse::new()
            .set_job_complete(true)
            .set_job_reference(job_ref.clone())
            .set_schema(TableSchema::new())
            .set_page_token("some_page_token")
            .set_rows([wkt::Struct::new()])
            .set_cache_hit(true);

        let query = Query {
            job_service: job_service.clone(),
            job_ref: Some(job_ref),
            completed: true,
            initial_job: None,
            initial_response: Some(query_res),
            query_template: Some(RunQuery::new(job_service.clone(), "SELECT 1".to_string())),
            retry_policy: Arc::new(create_test_retry_policy()),
            backoff_policy: Arc::new(create_test_retry_backoff_policy()),
            retry_state: RetryState::new(true),
        };

        let completed = query.until_done().await?;
        assert_eq!(completed.job_ref.as_ref().unwrap().job_id, "some_job_id");
        assert_eq!(completed.page_token, Some("some_page_token".to_string()));
        assert_eq!(completed.cached_rows.len(), 1);

        let metadata = completed.metadata();
        assert_eq!(metadata.cache_hit, Some(true));
        assert_eq!(metadata.job_complete, Some(true));
        assert_eq!(metadata.page_token, "some_page_token".to_string());

        Ok(())
    }

    #[tokio::test]
    async fn test_query_until_done_polls_success() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_get_query_results()
            .returning(|req, _| {
                assert_eq!(req.project_id, "some_project");
                assert_eq!(req.job_id, "some_job_id");
                assert_eq!(req.max_results, Some(0));
                assert_eq!(req.location, "us-central1");
                let res = GetQueryResultsResponse::new()
                    .set_job_complete(true)
                    .set_job_reference(JobReference::new().set_job_id(req.job_id))
                    .set_schema(TableSchema::new())
                    .set_page_token("")
                    .set_rows(vec![wkt::Struct::new(), wkt::Struct::new()])
                    .set_cache_hit(false);
                Ok(Response::from(res))
            })
            .times(1);
        let job_service = create_job_service(mock);
        let job_ref = JobReference::new()
            .set_project_id("some_project")
            .set_job_id("some_job_id")
            .set_location("us-central1");

        let query = Query {
            job_service: job_service.clone(),
            job_ref: Some(job_ref),
            completed: false,
            initial_job: None,
            initial_response: None,
            query_template: Some(RunQuery::new(job_service.clone(), "SELECT 1".to_string())),
            retry_policy: Arc::new(create_test_retry_policy()),
            backoff_policy: Arc::new(create_test_retry_backoff_policy()),
            retry_state: RetryState::new(true),
        };

        let completed = query.until_done().await?;
        assert_eq!(completed.job_ref.as_ref().unwrap().job_id, "some_job_id");
        assert_eq!(completed.page_token, None);
        assert_eq!(completed.cached_rows.len(), 2);

        let metadata = completed.metadata();
        assert_eq!(metadata.cache_hit, Some(false));
        assert_eq!(metadata.job_complete, Some(true));
        assert_eq!(metadata.page_token, "".to_string());

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_poll_query_results_loops_until_complete() -> TestResult {
        let mut mock = MockJobService::new();
        let mut backoff_policy = create_test_backoff_policy();
        backoff_policy
            .expect_wait_period()
            .times(2)
            .return_const(Duration::from_millis(1));

        let mut seq = mockall::Sequence::new();

        mock.expect_get_query_results()
            .in_sequence(&mut seq)
            .times(2)
            .returning(|_, _| {
                Ok(Response::from(
                    GetQueryResultsResponse::new().set_job_complete(false),
                ))
            });

        mock.expect_get_query_results()
            .in_sequence(&mut seq)
            .times(1)
            .returning(|_, _| {
                Ok(Response::from(
                    GetQueryResultsResponse::new().set_job_complete(true),
                ))
            });

        let job_service = create_job_service(mock);
        let job_ref = JobReference::new()
            .set_project_id("some_project")
            .set_job_id("some_job_id");

        let res = poll_query_results(
            &job_service,
            &job_ref,
            Arc::new(backoff_policy),
            Arc::new(create_test_retry_policy()),
            Arc::new(create_test_retry_backoff_policy()),
        )
        .await?;

        assert!(res.job_complete.unwrap(), "{res:?}");

        Ok(())
    }

    #[tokio::test]
    async fn test_query_until_done_job_failed_error() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_get_query_results().returning(|req, _| {
            assert_eq!(req.project_id, "some_project");
            assert_eq!(req.job_id, "some_job_id");
            assert_eq!(req.max_results, Some(0));
            let err_proto = ErrorProto::new()
                .set_reason("invalidQuery")
                .set_message("Syntax error");
            let res = GetQueryResultsResponse::new().set_errors(vec![err_proto]);
            Ok(Response::from(res))
        });
        let job_service = create_job_service(mock);
        let job_ref = JobReference::new()
            .set_project_id("some_project")
            .set_job_id("some_job_id");

        let query = Query {
            job_service: job_service.clone(),
            job_ref: Some(job_ref),
            completed: false,
            initial_job: None,
            initial_response: None,
            query_template: Some(RunQuery::new(job_service.clone(), "SELECT 1".to_string())),
            retry_policy: Arc::new(create_test_retry_policy()),
            backoff_policy: Arc::new(create_test_retry_backoff_policy()),
            retry_state: RetryState::new(true),
        };

        let err = query.until_done().await.unwrap_err();
        let errors = match err {
            QueryError::JobFailed { errors } => errors,
            _ => panic!("expected QueryError::JobFailed, got {err:?}"),
        };
        assert_eq!(
            errors,
            [ErrorProto::new()
                .set_reason("invalidQuery")
                .set_message("Syntax error")]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_query_until_done_rpc_error() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_get_query_results().returning(|req, _| {
            assert_eq!(req.project_id, "some_project");
            assert_eq!(req.job_id, "some_job_id");
            assert_eq!(req.max_results, Some(0));
            let status = Status::default()
                .set_code(Code::InvalidArgument)
                .set_message("simulated bad request");
            Err(GaxError::service(status))
        });
        let job_service = create_job_service(mock);
        let job_ref = JobReference::new()
            .set_project_id("some_project")
            .set_job_id("some_job_id");

        let query = Query {
            job_service: job_service.clone(),
            job_ref: Some(job_ref),
            completed: false,
            initial_job: None,
            initial_response: None,
            query_template: Some(RunQuery::new(job_service.clone(), "SELECT 1".to_string())),
            retry_policy: Arc::new(create_test_retry_policy()),
            backoff_policy: Arc::new(create_test_retry_backoff_policy()),
            retry_state: RetryState::new(true),
        };

        let err = query.until_done().await.unwrap_err();
        let source = match err {
            QueryError::Rpc { source } => source,
            _ => panic!("expected QueryError::Rpc, got {err:?}"),
        };
        assert_eq!(source.status().unwrap().code, Code::InvalidArgument);

        Ok(())
    }

    #[tokio::test]
    async fn test_complete_query_read() -> TestResult {
        let job_service = create_job_service(MockJobService::new());
        let job_ref = JobReference::new()
            .set_project_id("some_project")
            .set_job_id("some_job_id");
        let schema = TableSchema::new().set_fields([TableFieldSchema::new()
            .set_name("name")
            .set_type("STRING")
            .set_mode("NULLABLE")]);
        let row = serde_json::Map::from_iter([(
            "f".to_string(),
            serde_json::json!([{ "v": "test_name" }]),
        )]);
        let query_res = QueryResponse::new()
            .set_job_complete(true)
            .set_job_reference(job_ref.clone())
            .set_schema(schema)
            .set_rows(vec![row]);

        let complete_query = CompleteQuery::from_query_response(
            job_service,
            Some(job_ref),
            query_res,
            Arc::new(create_test_retry_policy()),
            Arc::new(create_test_retry_backoff_policy()),
        );

        let mut iter = complete_query.read();
        let row = iter.next().await.expect("should return first row")?;
        assert_eq!(row.get::<String, _>("name"), "test_name");
        assert!(iter.next().await.is_none(), "{iter:?}");

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_query_until_done_reissue_on_retryable_job_failed() -> TestResult {
        let mut mock = MockJobService::new();
        let mut seq = mockall::Sequence::new();

        mock.expect_get_query_results()
            .in_sequence(&mut seq)
            .times(1)
            .returning(|req, _| {
                assert_eq!(req.job_id, "initial_job_id");
                let err_proto = ErrorProto::new()
                    .set_reason("backendError")
                    .set_message("temporary server issue");
                let res = GetQueryResultsResponse::new().set_errors(vec![err_proto]);
                Ok(Response::from(res))
            });

        mock.expect_query()
            .in_sequence(&mut seq)
            .times(1)
            .returning(|req, _| {
                let req_id = &req.query_request.as_ref().unwrap().request_id;
                assert!(req_id.starts_with(JOB_ID_PREFIX));
                assert!(uuid::Uuid::parse_str(&req_id[JOB_ID_PREFIX.len()..]).is_ok());
                let new_job_ref = JobReference::new()
                    .set_project_id("some_project")
                    .set_job_id("reissued_job_id");
                Ok(Response::from(
                    QueryResponse::new()
                        .set_job_complete(false)
                        .set_job_reference(new_job_ref)
                        .set_schema(TableSchema::new()),
                ))
            });

        mock.expect_get_query_results()
            .in_sequence(&mut seq)
            .times(1)
            .returning(|req, _| {
                assert_eq!(req.job_id, "reissued_job_id");
                let res = GetQueryResultsResponse::new()
                    .set_job_complete(true)
                    .set_schema(TableSchema::new());
                Ok(Response::from(res))
            });

        let job_service = create_job_service(mock);
        let job_ref = JobReference::new()
            .set_project_id("some_project")
            .set_job_id("initial_job_id");

        let run_query = RunQuery::new(job_service.clone(), "SELECT 1".to_string())
            .with_project_id("some_project");

        let query = Query {
            job_service,
            job_ref: Some(job_ref),
            completed: false,
            initial_job: None,
            initial_response: None,
            query_template: Some(run_query),
            retry_policy: Arc::new(create_test_retry_policy()),
            backoff_policy: Arc::new(create_test_retry_backoff_policy()),
            retry_state: RetryState::new(true),
        };

        let completed = query.until_done().await?;
        assert_eq!(
            completed.job_ref.as_ref().unwrap().job_id,
            "reissued_job_id"
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_query_until_done_reissue_retry_exhausted() -> TestResult {
        let mut mock = MockJobService::new();
        let mut seq = mockall::Sequence::new();

        // First poll on initial_job_id fails with retryable error (attempt 0 -> 1)
        mock.expect_get_query_results()
            .in_sequence(&mut seq)
            .times(1)
            .returning(|req, _| {
                assert_eq!(req.job_id, "initial_job_id");
                let err_proto = ErrorProto::new()
                    .set_reason("backendError")
                    .set_message("first temporary server issue");
                let res = GetQueryResultsResponse::new().set_errors(vec![err_proto]);
                Ok(Response::from(res))
            });

        // Reissue succeeds and returns reissued_job_id
        mock.expect_query()
            .in_sequence(&mut seq)
            .times(1)
            .returning(|req, _| {
                let req_id = &req.query_request.as_ref().unwrap().request_id;
                assert!(req_id.starts_with(JOB_ID_PREFIX));
                assert!(uuid::Uuid::parse_str(&req_id[JOB_ID_PREFIX.len()..]).is_ok());
                let new_job_ref = JobReference::new()
                    .set_project_id("some_project")
                    .set_job_id("reissued_job_id");
                Ok(Response::from(
                    QueryResponse::new()
                        .set_job_complete(false)
                        .set_job_reference(new_job_ref)
                        .set_schema(TableSchema::new()),
                ))
            });

        // Second poll on reissued_job_id fails with retryable error, but attempt limit (1) is exhausted!
        mock.expect_get_query_results()
            .in_sequence(&mut seq)
            .times(1)
            .returning(|req, _| {
                assert_eq!(req.job_id, "reissued_job_id");
                let err_proto = ErrorProto::new()
                    .set_reason("backendError")
                    .set_message("second temporary server issue");
                let res = GetQueryResultsResponse::new().set_errors(vec![err_proto]);
                Ok(Response::from(res))
            });

        let job_service = create_job_service(mock);
        let job_ref = JobReference::new()
            .set_project_id("some_project")
            .set_job_id("initial_job_id");

        let run_query = RunQuery::new(job_service.clone(), "SELECT 1".to_string())
            .with_project_id("some_project");

        let policy = Arc::new(RetryableErrors.with_attempt_limit(1));

        let query = Query {
            job_service,
            job_ref: Some(job_ref),
            completed: false,
            initial_job: None,
            initial_response: None,
            query_template: Some(run_query),
            retry_policy: policy,
            backoff_policy: Arc::new(create_test_retry_backoff_policy()),
            retry_state: RetryState::new(true),
        };

        let err = query.until_done().await.unwrap_err();
        let errors = match err {
            QueryError::JobFailed { errors } => errors,
            _ => panic!("expected QueryError::JobFailed, got {err:?}"),
        };
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].reason, "backendError");
        assert_eq!(errors[0].message, "second temporary server issue");

        Ok(())
    }
}
