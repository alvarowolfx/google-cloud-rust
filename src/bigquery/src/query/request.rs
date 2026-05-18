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

use google_cloud_bigquery_v2::model::{
    DataFormatOptions, Job, JobConfiguration, JobConfigurationQuery, PostQueryRequest,
};

/// A trait to convert types into a `PostQueryRequest` for fast queries.
pub trait IntoPostQueryRequest {
    /// Converts the type into a `PostQueryRequest`.
    fn into_post_query_request(self) -> PostQueryRequest;
}

impl IntoPostQueryRequest for String {
    fn into_post_query_request(self) -> PostQueryRequest {
        let req = google_cloud_bigquery_v2::model::QueryRequest::new()
            .set_query(self)
            .set_format_options(DataFormatOptions::new().set_use_int64_timestamp(true))
            .set_use_legacy_sql(false);
        PostQueryRequest::new().set_query_request(req)
    }
}

impl IntoPostQueryRequest for &str {
    fn into_post_query_request(self) -> PostQueryRequest {
        self.to_string().into_post_query_request()
    }
}

impl IntoPostQueryRequest for google_cloud_bigquery_v2::model::QueryRequest {
    fn into_post_query_request(self) -> PostQueryRequest {
        let req = self.set_format_options(DataFormatOptions::new().set_use_int64_timestamp(true));
        PostQueryRequest::new().set_query_request(req)
    }
}

impl IntoPostQueryRequest for PostQueryRequest {
    fn into_post_query_request(self) -> PostQueryRequest {
        self
    }
}

/// A trait to convert types into a `Job` for advanced background query jobs.
pub trait IntoJob {
    /// Converts the type into a `Job`.
    fn into_job(self) -> Job;
}

impl IntoJob for Job {
    fn into_job(self) -> Job {
        self
    }
}

impl IntoJob for JobConfigurationQuery {
    fn into_job(self) -> Job {
        let config = JobConfiguration::new().set_query(self);
        Job::new().set_configuration(config)
    }
}
