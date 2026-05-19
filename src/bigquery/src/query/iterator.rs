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

use crate::query::{QueryMetadata, Row};

/// Represents errors that can occur when reading query results.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Only complete Query Jobs with schema can be read.
    #[error("Only complete Query Jobs with schema can be read.")]
    MissingSchema,
}

/// A single page of rows returned by a query.
#[derive(Clone, Debug)]
pub struct ResultsPage {
    /// The rows contained in this page.
    pub rows: Vec<Row>,
    /// The token to request the next page, or `None` if this is the last page.
    pub page_token: Option<String>,
    /// The metadata associated with this page of results.
    pub metadata: QueryMetadata,
}

impl google_cloud_gax::paginator::internal::PageableResponse for ResultsPage {
    type PageItem = Row;

    fn items(self) -> Vec<Self::PageItem> {
        self.rows
    }

    fn next_page_token(&self) -> String {
        self.page_token.clone().unwrap_or_default()
    }
}
