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
use arrow::record_batch::RecordBatch;
use google_cloud_bigquery_read::client::Read as ReadClient;
use google_cloud_bigquery_read::model::{
    ArrowSerializationOptions, CreateReadSessionRequest, DataFormat, ReadRowsResponse, ReadSession,
    arrow_serialization_options::CompressionCodec, read_rows_response::Rows,
    read_rows_response::Schema as ReadRowsResponseSchema, read_session::TableReadOptions,
};
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::JobReference;
use google_cloud_gax::streaming::ResponseReceiver;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Default threshold below which queries stick to REST pagination.
pub(crate) const DEFAULT_REST_ROW_THRESHOLD: u64 = 10_000;

/// Default threshold above which queries switch from single job stream to multi-stream ReadSession.
pub(crate) const DEFAULT_MULTI_STREAM_THRESHOLD: u64 = 5_000_000;

pub(crate) fn rest_row_threshold() -> u64 {
    std::env::var("GOOGLE_CLOUD_RUST_BIGQUERY_REST_THRESHOLD")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_REST_ROW_THRESHOLD)
}

pub(crate) fn multi_stream_threshold() -> u64 {
    std::env::var("GOOGLE_CLOUD_RUST_BIGQUERY_MULTI_STREAM_THRESHOLD")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_MULTI_STREAM_THRESHOLD)
}

#[derive(Debug)]
pub(crate) enum ReaderState {
    Unopened,
    SingleStream {
        stream: ResponseReceiver<ReadRowsResponse>,
    },
    MultiStream {
        rx: mpsc::UnboundedReceiver<Result<RecordBatch, RowError>>,
    },
    Completed,
}

#[derive(Debug)]
pub(crate) struct StorageReader {
    pub(crate) read_client: Arc<ReadClient>,
    pub(crate) job_service: Option<Arc<JobService>>,
    pub(crate) job_ref: JobReference,
    pub(crate) estimated_total_rows: Option<u64>,
    pub(crate) decoder: crate::query::arrow::ArrowStreamDecoder,
    pub(crate) state: ReaderState,
    pub(crate) buffer: VecDeque<RecordBatch>,
    pub(crate) multi_stream_threshold: u64,
}

impl StorageReader {
    pub(crate) fn new(
        read_client: Arc<ReadClient>,
        job_ref: JobReference,
        job_service: Option<Arc<JobService>>,
        estimated_total_rows: Option<u64>,
    ) -> Self {
        Self {
            read_client,
            job_service,
            job_ref,
            estimated_total_rows,
            decoder: crate::query::arrow::ArrowStreamDecoder::new(),
            state: ReaderState::Unopened,
            buffer: VecDeque::new(),
            multi_stream_threshold: multi_stream_threshold(),
        }
    }

    /// Returns true if a stream has already been opened.
    pub(crate) fn is_stream_opened(&self) -> bool {
        !matches!(self.state, ReaderState::Unopened)
    }

    /// Retrieves the next [`RecordBatch`] from the buffered batches, or fetches from the streams.
    pub(crate) async fn next_batch(&mut self) -> Result<Option<RecordBatch>, RowError> {
        if let Some(batch) = self.buffer.pop_front() {
            return Ok(Some(batch));
        }

        loop {
            match &mut self.state {
                ReaderState::Completed => return Ok(None),
                ReaderState::Unopened => {
                    if self
                        .estimated_total_rows
                        .map(|r| r > self.multi_stream_threshold)
                        .unwrap_or(false)
                        && let Some(ref job_service) = self.job_service
                    {
                        let job_service = job_service.clone();
                        let read_client = self.read_client.clone();
                        let project_id = self.job_ref.project_id.clone();
                        let job_id = self.job_ref.job_id.clone();
                        let location = self.job_ref.location.clone();

                        if let Ok(Some(rx)) = Self::try_create_multi_stream_session(
                            read_client,
                            job_service,
                            project_id,
                            job_id,
                            location,
                        )
                        .await
                        {
                            self.state = ReaderState::MultiStream { rx };
                            continue;
                        }
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

                    self.state = ReaderState::SingleStream { stream };
                }
                ReaderState::SingleStream { stream } => {
                    match stream.recv().await {
                        Some(Ok(response)) => {
                            if let Some(ReadRowsResponseSchema::ArrowSchema(arrow_schema)) =
                                response.schema
                            {
                                self.decoder
                                    .set_schema_bytes(&arrow_schema.serialized_schema)?;
                            }

                            let maybe_batch =
                                if let Some(Rows::ArrowRecordBatch(batch)) = response.rows {
                                    self.decoder
                                        .decode_batch(&batch.serialized_record_batch)?
                                        .filter(|record_batch| record_batch.num_rows() > 0)
                                } else {
                                    None
                                };

                            // Check if estimated total rows exceeds threshold for multi-stream acceleration
                            let total_estimated =
                                response.total_estimated_row_count.unwrap_or(0) as u64;
                            if total_estimated > self.multi_stream_threshold
                                && let Some(ref job_service) = self.job_service
                            {
                                let job_service = job_service.clone();
                                let read_client = self.read_client.clone();
                                let project_id = self.job_ref.project_id.clone();
                                let job_id = self.job_ref.job_id.clone();
                                let location = self.job_ref.location.clone();

                                if let Ok(Some(rx)) = Self::try_create_multi_stream_session(
                                    read_client,
                                    job_service,
                                    project_id,
                                    job_id,
                                    location,
                                )
                                .await
                                {
                                    self.state = ReaderState::MultiStream { rx };
                                    continue;
                                }
                            }

                            if let Some(batch) = maybe_batch {
                                return Ok(Some(batch));
                            }
                        }
                        None => {
                            self.state = ReaderState::Completed;
                            return Ok(None);
                        }
                        Some(Err(err)) => {
                            self.state = ReaderState::Completed;
                            return Err(RowError::Rpc { source: err });
                        }
                    }
                }
                ReaderState::MultiStream { rx } => match rx.recv().await {
                    Some(Ok(batch)) => {
                        if batch.num_rows() > 0 {
                            return Ok(Some(batch));
                        }
                    }
                    Some(Err(err)) => {
                        self.state = ReaderState::Completed;
                        return Err(err);
                    }
                    None => {
                        self.state = ReaderState::Completed;
                        return Ok(None);
                    }
                },
            }
        }
    }

    async fn try_create_multi_stream_session(
        read_client: Arc<ReadClient>,
        job_service: Arc<JobService>,
        project_id: String,
        job_id: String,
        location: Option<String>,
    ) -> Result<Option<mpsc::UnboundedReceiver<Result<RecordBatch, RowError>>>, RowError> {
        // 1. Fetch destination table reference from query job
        let mut req = job_service
            .get_job()
            .set_job_id(job_id)
            .set_project_id(project_id.clone());

        if let Some(loc) = location {
            req = req.set_location(loc);
        }

        let Ok(job) = req.send().await else {
            return Ok(None);
        };

        let dest_table = job
            .configuration
            .and_then(|c| c.query)
            .and_then(|q| q.destination_table);

        let Some(dest_table) = dest_table else {
            return Ok(None);
        };

        let table_path = format!(
            "projects/{}/datasets/{}/tables/{}",
            dest_table.project_id, dest_table.dataset_id, dest_table.table_id
        );

        // 2. Create ReadSession on the destination table with LZ4 buffer compression
        let session_req = CreateReadSessionRequest::new()
            .set_parent(format!("projects/{project_id}"))
            .set_read_session(
                ReadSession::new()
                    .set_table(table_path)
                    .set_data_format(DataFormat::Arrow)
                    .set_read_options(
                        TableReadOptions::new().set_arrow_serialization_options(
                            ArrowSerializationOptions::new()
                                .set_buffer_compression(CompressionCodec::Lz4Frame),
                        ),
                    ),
            );

        let Ok(session) = read_client
            .create_read_session()
            .with_request(session_req)
            .send()
            .await
        else {
            return Ok(None);
        };

        if session.streams.len() <= 1 {
            return Ok(None);
        }

        // 3. Spawn parallel worker tasks for each stream into an unbounded channel
        let (tx, rx) = mpsc::unbounded_channel();
        for stream in session.streams {
            let read_client = read_client.clone();
            let stream_name = stream.name;
            let tx = tx.clone();
            tokio::spawn(async move {
                let stream_res = read_client
                    .read_rows()
                    .set_read_stream(stream_name)
                    .set_offset(0)
                    .send()
                    .await;

                let mut s = match stream_res {
                    Ok(s) => s,
                    Err(e) => {
                        let _ = tx.send(Err(RowError::Rpc { source: e }));
                        return;
                    }
                };

                let mut decoder = crate::query::arrow::ArrowStreamDecoder::new();
                while let Some(res) = s.recv().await {
                    match res {
                        Ok(response) => {
                            if let Some(ReadRowsResponseSchema::ArrowSchema(arrow_schema)) =
                                response.schema
                                && let Err(e) =
                                    decoder.set_schema_bytes(&arrow_schema.serialized_schema)
                            {
                                let _ = tx.send(Err(e));
                                return;
                            }
                            if let Some(Rows::ArrowRecordBatch(batch)) = response.rows {
                                match decoder.decode_batch(&batch.serialized_record_batch) {
                                    Ok(Some(rb)) => {
                                        if rb.num_rows() > 0 && tx.send(Ok(rb)).is_err() {
                                            return;
                                        }
                                    }
                                    Ok(None) => {}
                                    Err(e) => {
                                        let _ = tx.send(Err(e));
                                        return;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(Err(RowError::Rpc { source: e }));
                            return;
                        }
                    }
                }
            });
        }

        Ok(Some(rx))
    }
}
