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

use crate::error::RowError;
use crate::query::CompleteQuery;
use crate::query::query_handle::CachedData;
use crate::query::storage_reader::StorageReader;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use std::collections::VecDeque;
use std::io::{Cursor, Read};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, channel};

/// ABI-compatible Apache Arrow C Stream structure conforming to the Arrow C Stream Interface specification.
///
/// This type can be passed directly across FFI boundaries to Polars, DuckDB, PyArrow,
/// or DataFusion without requiring a dependency on a specific Rust `arrow` crate version.
pub type ArrowArrayStream = FFI_ArrowArrayStream;

struct BigQueryRecordBatchReader {
    schema: SchemaRef,
    rx: Receiver<Result<RecordBatch, RowError>>,
    _handle: tokio::task::JoinHandle<()>,
}

impl Iterator for BigQueryRecordBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.rx.blocking_recv() {
            Some(Ok(batch)) => Some(Ok(batch)),
            Some(Err(e)) => Some(Err(ArrowError::ExternalError(Box::new(e)))),
            None => None,
        }
    }
}

impl RecordBatchReader for BigQueryRecordBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Converts a [`CompleteQuery`] into an Apache Arrow C Stream interface (`ArrowArrayStream`).
pub(crate) async fn query_to_arrow_c_stream(
    q: CompleteQuery,
) -> Result<ArrowArrayStream, RowError> {
    let (mut initial_batches, initial_schema) = match q.cached_data {
        CachedData::Arrow {
            serialized_record_batch,
            serialized_schema,
        } => {
            let reader = StreamReader::try_new(
                Cursor::new(serialized_schema.as_ref())
                    .chain(Cursor::new(serialized_record_batch.as_ref())),
                None,
            )
            .map_err(|e| {
                RowError::InvalidRowFormat(format!("failed to parse arrow schema: {e}"))
            })?;
            let schema = reader.schema();
            let batches = reader
                .collect::<std::result::Result<VecDeque<_>, _>>()
                .map_err(|e| {
                    RowError::InvalidRowFormat(format!("failed to parse arrow batches: {e}"))
                })?;
            (batches, Some(schema))
        }
        CachedData::Rows(_) => (VecDeque::new(), None),
    };

    let mut storage_reader = match (q.read_client, q.job_ref) {
        (Some(read_client), Some(job_ref))
            if q.page_token.is_some() || initial_batches.is_empty() =>
        {
            Some(StorageReader::new(
                read_client,
                job_ref,
                Some(q.job_service.clone()),
                q.metadata.total_rows,
            ))
        }
        _ => None,
    };

    let mut preloaded_batch = None;
    let schema = if let Some(schema) = initial_schema {
        schema
    } else if let Some(ref mut reader) = storage_reader {
        match reader.next_batch().await {
            Ok(Some(batch)) => {
                let schema = batch.schema();
                preloaded_batch = Some(batch);
                schema
            }
            Ok(None) => Arc::new(Schema::empty()),
            Err(e) => return Err(e),
        }
    } else {
        Arc::new(Schema::empty())
    };

    let (tx, rx) = channel(64);

    let handle = tokio::spawn(async move {
        while let Some(batch) = initial_batches.pop_front() {
            if tx.send(Ok(batch)).await.is_err() {
                return;
            }
        }

        if let Some(batch) = preloaded_batch
            && tx.send(Ok(batch)).await.is_err()
        {
            return;
        }

        if let Some(mut reader) = storage_reader {
            loop {
                match reader.next_batch().await {
                    Ok(Some(batch)) => {
                        if tx.send(Ok(batch)).await.is_err() {
                            return;
                        }
                    }
                    Ok(None) => return,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                }
            }
        }
    });

    let reader = BigQueryRecordBatchReader {
        schema,
        rx,
        _handle: handle,
    };

    Ok(FFI_ArrowArrayStream::new(Box::new(reader)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::tests::{MockJobService, create_job_service};
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ffi_stream::ArrowArrayStreamReader;
    use arrow::ipc::writer::StreamWriter;
    use google_cloud_bigquery_v2::model::{JobReference, QueryResponse};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_c_stream_from_cached_arrow() -> anyhow::Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            ],
        )?;

        let mut batch_buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut batch_buf, &schema)?;
            writer.write(&batch)?;
            writer.finish()?;
        }

        let job_service = create_job_service(MockJobService::new());
        let job_ref = JobReference::new()
            .set_project_id("test-proj")
            .set_job_id("test-job");
        let query_res = QueryResponse::new()
            .set_job_complete(true)
            .set_job_reference(job_ref);

        let mut complete_query = CompleteQuery::from_query_response(job_service, query_res, None);
        complete_query.cached_data = CachedData::Arrow {
            serialized_record_batch: bytes::Bytes::from(batch_buf),
            serialized_schema: bytes::Bytes::new(),
        };

        let mut c_stream = complete_query.to_arrow_c_stream().await?;

        // Consume via Arrow C Stream Interface ABI on current or blocking thread
        let reader = unsafe { ArrowArrayStreamReader::from_raw(&mut c_stream as *mut _) }?;
        let schema_out = reader.schema();
        assert_eq!(schema_out.fields().len(), 2);
        assert_eq!(schema_out.field(0).name(), "id");
        assert_eq!(schema_out.field(1).name(), "name");

        let batches_out: Vec<RecordBatch> = reader.collect::<std::result::Result<Vec<_>, _>>()?;
        assert_eq!(batches_out.len(), 1);
        assert_eq!(batches_out[0].num_rows(), 3);

        let ids = batches_out[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);
        assert_eq!(ids.value(2), 3);

        let names = batches_out[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "Alice");
        assert_eq!(names.value(1), "Bob");
        assert_eq!(names.value(2), "Charlie");

        Ok(())
    }
}
