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

#![allow(unused_imports, dead_code)]

pub(crate) mod execution;
mod handle;
mod iterator;
mod job_reference;
mod request;
mod row;
mod run_query;
mod schema;

pub(crate) use handle::{
    CompleteQuery, Error as QueryError, Query, QueryCreationMetadata, QueryMetadata,
};
pub(crate) use iterator::{Error as IteratorError, RowIterator};
pub(crate) use job_reference::JobReference;
pub(crate) use request::QueryRequest;
pub(crate) use row::Row;
pub(crate) use run_query::RunQuery;
pub(crate) use schema::Schema;
