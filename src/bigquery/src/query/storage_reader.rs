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
    DataFormat, ReadRowsResponse, ReadSession, ReadStream, read_rows_response::Rows,
    read_session::Schema as ReadSessionSchema,
};
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::{JobReference, TableReference};
use google_cloud_gax::streaming::ResponseReceiver;
use std::collections::VecDeque;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct StorageReader {
    pub(crate) read_client: Arc<ReadClient>,
    pub(crate) job_service: Arc<JobService>,
    pub(crate) job_ref: JobReference,
    pub(crate) destination_table: Option<TableReference>,
    pub(crate) session: Option<ReadSession>,
    pub(crate) serialized_schema: Option<Vec<u8>>,
    pub(crate) arrow_schema: Option<arrow::datatypes::SchemaRef>,
    pub(crate) streams: Vec<ReadStream>,
    pub(crate) current_stream_idx: usize,
    pub(crate) current_stream: Option<ResponseReceiver<ReadRowsResponse>>,
    pub(crate) batch_buffer: VecDeque<RecordBatch>,
    pub(crate) initialized: bool,
}

impl StorageReader {
    pub(crate) fn new(
        read_client: Arc<ReadClient>,
        job_service: Arc<JobService>,
        job_ref: JobReference,
        destination_table: Option<TableReference>,
    ) -> Self {
        Self {
            read_client,
            job_service,
            job_ref,
            destination_table,
            session: None,
            serialized_schema: None,
            arrow_schema: None,
            streams: Vec::new(),
            current_stream_idx: 0,
            current_stream: None,
            batch_buffer: VecDeque::new(),
            initialized: false,
        }
    }

    /// Fetches destination table from the BigQuery Job if not already known.
    async fn resolve_destination_table(&mut self) -> Result<String, RowError> {
        if let Some(table_ref) = &self.destination_table {
            return Ok(format_table_resource(table_ref));
        }

        let mut req = self
            .job_service
            .get_job()
            .set_project_id(self.job_ref.project_id.clone())
            .set_job_id(self.job_ref.job_id.clone());
        if let Some(loc) = self.job_ref.location.clone() {
            req = req.set_location(loc);
        }

        let job = req.send().await?;
        let dst = job
            .configuration
            .as_ref()
            .and_then(|c| c.query.as_ref())
            .and_then(|q| q.destination_table.clone())
            .ok_or_else(|| {
                RowError::InvalidRowFormat("query job has no destination table to read".into())
            })?;

        let formatted = format_table_resource(&dst);
        self.destination_table = Some(dst);
        Ok(formatted)
    }

    /// Initializes the read session with the BigQuery Storage Read API.
    pub(crate) async fn init_session(&mut self) -> Result<(), RowError> {
        if self.initialized {
            return Ok(());
        }

        let table_resource = self.resolve_destination_table().await?;
        let project_id = &self.job_ref.project_id;

        let read_session = ReadSession::new()
            .set_data_format(DataFormat::Arrow)
            .set_table(table_resource);

        let session = self
            .read_client
            .create_read_session()
            .set_parent(format!("projects/{project_id}"))
            .set_read_session(read_session)
            .send()
            .await?;

        let serialized_schema = match &session.schema {
            Some(ReadSessionSchema::ArrowSchema(arrow_schema)) => {
                arrow_schema.serialized_schema.to_vec()
            }
            _ => {
                return Err(RowError::InvalidRowFormat(
                    "read session did not return an Arrow schema".into(),
                ));
            }
        };

        let arrow_schema = arrow::ipc::reader::StreamReader::try_new(
            std::io::Cursor::new(&serialized_schema),
            None,
        )
        .map(|r| r.schema())
        .ok();

        self.streams = session.streams.clone();
        self.serialized_schema = Some(serialized_schema);
        self.arrow_schema = arrow_schema;
        self.session = Some(session);
        self.initialized = true;

        Ok(())
    }

    /// Retrieves the next [`RecordBatch`] from the buffered batches, or fetches from the streams.
    pub(crate) async fn next_batch(&mut self) -> Result<Option<RecordBatch>, RowError> {
        if !self.initialized {
            self.init_session().await?;
        }

        if let Some(batch) = self.batch_buffer.pop_front() {
            return Ok(Some(batch));
        }

        let serialized_schema = self
            .serialized_schema
            .as_ref()
            .expect("serialized schema must be initialized");

        loop {
            if self.current_stream.is_none() {
                if self.current_stream_idx >= self.streams.len() {
                    return Ok(None);
                }

                let stream_name = self.streams[self.current_stream_idx].name.clone();
                let stream = self
                    .read_client
                    .read_rows()
                    .set_read_stream(stream_name)
                    .set_offset(0)
                    .send()
                    .await?;

                self.current_stream = Some(stream);
                self.current_stream_idx += 1;
            }

            let stream = self.current_stream.as_mut().unwrap();
            match stream.recv().await {
                Some(Ok(response)) => {
                    if let Some(Rows::ArrowRecordBatch(batch)) = response.rows {
                        let record_batch = decode_arrow_batch(serialized_schema, &batch.serialized_record_batch)?;
                        if record_batch.num_rows() > 0 {
                            return Ok(Some(record_batch));
                        }
                    }
                }
                None => {
                    // Current stream ended, advance to next stream
                    self.current_stream = None;
                }
                Some(Err(err)) => {
                    self.current_stream = None;
                    return Err(RowError::Rpc { source: err });
                }
            }
        }
    }
}

fn format_table_resource(table_ref: &TableReference) -> String {
    format!(
        "projects/{}/datasets/{}/tables/{}",
        table_ref.project_id, table_ref.dataset_id, table_ref.table_id
    )
}
