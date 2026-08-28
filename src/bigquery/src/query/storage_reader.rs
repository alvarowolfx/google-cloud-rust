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
use crate::query::arrow::decode_arrow_batch;
use arrow::record_batch::RecordBatch;
use google_cloud_bigquery_read::client::Read as ReadClient;
use google_cloud_bigquery_read::model::{
    ReadRowsResponse, read_rows_response::Rows,
    read_rows_response::Schema as ReadRowsResponseSchema,
};
use google_cloud_bigquery_v2::model::JobReference;
use google_cloud_gax::streaming::ResponseReceiver;
use std::collections::VecDeque;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct StorageReader {
    pub(crate) read_client: Arc<ReadClient>,
    pub(crate) job_ref: JobReference,
    pub(crate) serialized_schema: Option<Vec<u8>>,
    pub(crate) arrow_schema: Option<arrow::datatypes::SchemaRef>,
    pub(crate) stream_opened: bool,
    pub(crate) current_stream: Option<ResponseReceiver<ReadRowsResponse>>,
    pub(crate) buffer: VecDeque<RecordBatch>,
}

impl StorageReader {
    pub(crate) fn new(read_client: Arc<ReadClient>, job_ref: JobReference) -> Self {
        Self {
            read_client,
            job_ref,
            serialized_schema: None,
            arrow_schema: None,
            stream_opened: false,
            current_stream: None,
            buffer: VecDeque::new(),
        }
    }

    /// Retrieves the next [`RecordBatch`] from the buffered batches, or fetches from the streams.
    pub(crate) async fn next_batch(&mut self) -> Result<Option<RecordBatch>, RowError> {
        if let Some(batch) = self.buffer.pop_front() {
            return Ok(Some(batch));
        }

        loop {
            if self.current_stream.is_none() {
                if self.stream_opened {
                    return Ok(None);
                }
                let project = &self.job_ref.project_id;
                let location = self.job_ref.location.as_deref().unwrap_or("US");
                let job_id = &self.job_ref.job_id;
                let stream_name = format!(
                    "projects/{project}/locations/{location}/jobs/{job_id}/streams/_default"
                );
                let stream = self
                    .read_client
                    .read_rows()
                    .set_read_stream(stream_name)
                    .set_offset(0)
                    .send()
                    .await?;

                self.current_stream = Some(stream);
                self.stream_opened = true;
            }

            let stream = self.current_stream.as_mut().unwrap();
            match stream.recv().await {
                Some(Ok(response)) => {
                    if let Some(ReadRowsResponseSchema::ArrowSchema(arrow_schema)) = response.schema
                    {
                        let schema_bytes = arrow_schema.serialized_schema.to_vec();
                        if let Ok(reader) = arrow::ipc::reader::StreamReader::try_new(
                            std::io::Cursor::new(&schema_bytes),
                            None,
                        ) {
                            self.arrow_schema = Some(reader.schema());
                        }
                        self.serialized_schema = Some(schema_bytes);
                    }
                    if let Some(Rows::ArrowRecordBatch(batch)) = response.rows {
                        let serialized_schema =
                            self.serialized_schema.as_ref().ok_or_else(|| {
                                RowError::InvalidRowFormat(
                                    "missing arrow schema in stream response".into(),
                                )
                            })?;
                        let record_batch =
                            decode_arrow_batch(serialized_schema, &batch.serialized_record_batch)?;
                        if record_batch.num_rows() > 0 {
                            return Ok(Some(record_batch));
                        }
                    }
                }
                None => {
                    self.current_stream = None;
                    return Ok(None);
                }
                Some(Err(err)) => {
                    self.current_stream = None;
                    return Err(RowError::Rpc { source: err });
                }
            }
        }
    }
}
