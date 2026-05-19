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

use google_cloud_bigquery_v2::model::{GetQueryResultsResponse, JobReference, QueryResponse};

/// Metadata associated with a fast-path query completion.
///
/// Wraps the raw service payload received when the query finished, either
/// immediately (`QueryResponse`) or after polling (`GetQueryResultsResponse`).
///
/// Standard users can call getters directly on this enum, while advanced
/// users can pattern match to access raw payload fields.
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum QueryMetadata {
    /// Raw response from a `jobs.query` call.
    JobsQuery(QueryResponse),
    /// Raw response from a `jobs.getQueryResults` polling call.
    GetQueryResultsResponse(GetQueryResultsResponse),
}

impl QueryMetadata {
    /// Returns the standard BigQuery Job Reference.
    pub fn job_reference(&self) -> Option<&JobReference> {
        match self {
            Self::JobsQuery(res) => res.job_reference.as_ref(),
            Self::GetQueryResultsResponse(res) => res.job_reference.as_ref(),
        }
    }

    /// Returns the stateless query ID, unique to the `jobs.query` API.
    pub fn query_id(&self) -> Option<&str> {
        match self {
            Self::JobsQuery(res) => Some(&res.query_id),
            Self::GetQueryResultsResponse(_) => None,
        }
    }

    /// Returns the total number of rows in the query results.
    pub fn total_rows(&self) -> Option<u64> {
        match self {
            Self::JobsQuery(res) => res.total_rows,
            Self::GetQueryResultsResponse(res) => res.total_rows,
        }
    }

    /// Returns the total number of bytes processed for this query.
    pub fn total_bytes_processed(&self) -> Option<i64> {
        match self {
            Self::JobsQuery(res) => res.total_bytes_processed,
            Self::GetQueryResultsResponse(res) => res.total_bytes_processed,
        }
    }

    /// Returns the total slot milliseconds billed for this query, if available.
    pub fn total_slot_ms(&self) -> Option<i64> {
        match self {
            Self::JobsQuery(res) => res.total_slot_ms,
            Self::GetQueryResultsResponse(_) => None,
        }
    }

    /// Returns whether the query results were read from the query cache.
    pub fn cache_hit(&self) -> Option<bool> {
        match self {
            Self::JobsQuery(res) => res.cache_hit,
            Self::GetQueryResultsResponse(res) => res.cache_hit,
        }
    }

    /// Returns the number of rows affected by a DML statement.
    pub fn num_dml_affected_rows(&self) -> Option<i64> {
        match self {
            Self::JobsQuery(res) => res.num_dml_affected_rows,
            Self::GetQueryResultsResponse(res) => res.num_dml_affected_rows,
        }
    }
}
