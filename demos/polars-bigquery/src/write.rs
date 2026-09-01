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

//! BigQuery Storage Write API support for Polars, supporting default, pending, and committed streams with offset controls.

use crate::{PyPolarsCredentialProvider, RUNTIME};
use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use google_cloud_auth::credentials::Credentials;
use google_cloud_bigquery::client::Write;
use google_cloud_bigquery::model::{ArrowRecordBatch, ArrowSchema};
use google_cloud_bigquery::write::arrow::{CommittedWriter, PendingWriter};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

/// Result of appending a batch of rows to a BigQuery write stream.
#[pyclass(name = "AppendResult")]
#[derive(Clone, Debug)]
pub struct PyAppendResult {
    /// Starting offset sent with the append request (None if offset was omitted).
    #[pyo3(get)]
    pub offset: Option<i64>,
    /// Number of rows written in this batch.
    #[pyo3(get)]
    pub rows_written: i64,
}

#[pymethods]
impl PyAppendResult {
    #[new]
    #[pyo3(signature = (offset=None, rows_written=0))]
    pub fn new(offset: Option<i64>, rows_written: i64) -> Self {
        Self {
            offset,
            rows_written,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "AppendResult(offset={:?}, rows_written={})",
            self.offset, self.rows_written
        )
    }
}

fn normalize_field(f: &arrow::datatypes::Field) -> (arrow::datatypes::Field, bool) {
    match f.data_type() {
        arrow::datatypes::DataType::Utf8View => (
            arrow::datatypes::Field::new(
                f.name(),
                arrow::datatypes::DataType::Utf8,
                f.is_nullable(),
            )
            .with_metadata(f.metadata().clone()),
            true,
        ),
        arrow::datatypes::DataType::BinaryView => (
            arrow::datatypes::Field::new(
                f.name(),
                arrow::datatypes::DataType::Binary,
                f.is_nullable(),
            )
            .with_metadata(f.metadata().clone()),
            true,
        ),
        _ => (f.clone(), false),
    }
}

/// Normalizes an Arrow schema by converting incompatible types (such as Utf8View/BinaryView)
/// into standard BigQuery Storage Write API compatible types (Utf8/Binary).
pub fn normalize_schema(schema: &Schema) -> (Arc<Schema>, bool) {
    let mut any_converted = false;
    let mut fields = Vec::with_capacity(schema.fields().len());
    for f in schema.fields().iter() {
        let (new_field, converted) = normalize_field(f.as_ref());
        if converted {
            any_converted = true;
        }
        fields.push(Arc::new(new_field));
    }
    if any_converted {
        (
            Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
            true,
        )
    } else {
        (Arc::new(schema.clone()), false)
    }
}

/// Casts batch columns to match target schema when view types (e.g. Utf8View) are present.
pub fn normalize_batch(
    batch: &RecordBatch,
    target_schema: &Arc<Schema>,
    needs_conversion: bool,
) -> Result<RecordBatch, ArrowError> {
    if !needs_conversion {
        return Ok(batch.clone());
    }
    let mut new_columns = Vec::with_capacity(batch.num_columns());
    for (col, field) in batch.columns().iter().zip(target_schema.fields().iter()) {
        if col.data_type() != field.data_type() {
            let casted = arrow_cast::cast(col, field.data_type())?;
            new_columns.push(casted);
        } else {
            new_columns.push(col.clone());
        }
    }
    RecordBatch::try_new(target_schema.clone(), new_columns)
}

/// Serializes an Arrow Schema into an Arrow IPC stream message format expected by BigQuery.
pub fn serialize_schema(schema: &Schema) -> Result<Vec<u8>, ArrowError> {
    let mut buf = Vec::new();
    let _ = StreamWriter::try_new(&mut buf, schema)?;
    Ok(buf)
}

/// Serializes an Arrow RecordBatch into an Arrow IPC stream message format expected by BigQuery.
/// Strips the leading schema message per BigQuery Storage Write API protocol specification.
pub fn serialize_batch(batch: &RecordBatch, schema_len: usize) -> Result<Vec<u8>, ArrowError> {
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())?;
    writer.write(batch)?;
    if buf.len() < schema_len {
        return Err(ArrowError::IpcError(
            "Serialized batch buffer is smaller than schema header".to_string(),
        ));
    }
    Ok(buf[schema_len..].to_vec())
}

#[derive(Clone, Hash, PartialEq, Eq)]
enum WriteClientCacheKey {
    Adc {
        project_id: Option<String>,
    },
    Custom {
        provider_ptr: usize,
        project_id: Option<String>,
    },
}

static WRITE_CLIENT_CACHE: LazyLock<tokio::sync::RwLock<HashMap<WriteClientCacheKey, Arc<Write>>>> =
    LazyLock::new(|| tokio::sync::RwLock::new(HashMap::new()));

async fn get_write_client(
    project_id: Option<String>,
    credential_provider: Option<PyObject>,
) -> PyResult<Arc<Write>> {
    let key = if let Some(ref provider) = credential_provider {
        WriteClientCacheKey::Custom {
            provider_ptr: provider.as_ptr() as usize,
            project_id: project_id.clone(),
        }
    } else {
        WriteClientCacheKey::Adc {
            project_id: project_id.clone(),
        }
    };

    let cached = {
        let lock = WRITE_CLIENT_CACHE.read().await;
        lock.get(&key).cloned()
    };

    if let Some(client) = cached {
        return Ok(client);
    }

    let mut builder = Write::builder();
    if let Some(provider) = credential_provider {
        let auth_provider = PyPolarsCredentialProvider::new(provider, project_id.clone());
        builder = builder.with_credentials(Credentials::from(auth_provider));
    }

    let client = builder
        .build()
        .await
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    let client = Arc::new(client);
    let mut lock = WRITE_CLIENT_CACHE.write().await;
    Ok(lock.entry(key).or_insert_with(|| client.clone()).clone())
}

pub fn extract_reader_from_capsule(data: &Bound<'_, PyAny>) -> PyResult<ArrowArrayStreamReader> {
    let capsule = if let Ok(capsule) = data.downcast::<PyCapsule>() {
        capsule.clone()
    } else if data.hasattr("__arrow_c_stream__")? {
        let res = data.call_method0("__arrow_c_stream__")?;
        res.downcast_into::<PyCapsule>()?
    } else if let Ok(collect_method) = data.getattr("collect") {
        let df = collect_method.call0()?;
        let res = df.call_method0("__arrow_c_stream__")?;
        res.downcast_into::<PyCapsule>()?
    } else {
        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
            "Expected PyCapsule, DataFrame, LazyFrame, or object with __arrow_c_stream__, got: {}",
            data.get_type().name()?
        )));
    };

    let raw_ptr = capsule.pointer() as *mut FFI_ArrowArrayStream;
    if raw_ptr.is_null() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Arrow C Stream capsule pointer is null",
        ));
    }
    unsafe { ArrowArrayStreamReader::from_raw(raw_ptr) }
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
}

/// A Python handle for a BigQuery Pending write stream (ACID transactional write).
#[pyclass(name = "PendingStream")]
pub struct PyPendingStream {
    writer: Arc<PendingWriter>,
    target_schema: Arc<Schema>,
    schema_len: usize,
    needs_conversion: bool,
    #[pyo3(get)]
    pub name: String,
}

#[pymethods]
impl PyPendingStream {
    /// Writes Arrow data from a PyCapsule Arrow C Stream or DataFrame to the pending stream.
    ///
    /// If `offset` is specified (`Some(val)`), BigQuery validates the exact offset and ensures idempotent writes.
    /// If `offset` is `None`, no offset is sent in the request proto, appending to the stream without offset check.
    #[pyo3(signature = (stream_capsule, offset=None))]
    pub fn write(
        &self,
        py: Python<'_>,
        stream_capsule: &Bound<'_, PyAny>,
        offset: Option<i64>,
    ) -> PyResult<PyAppendResult> {
        let mut reader = extract_reader_from_capsule(stream_capsule)?;
        let writer = self.writer.clone();
        let schema_len = self.schema_len;

        py.allow_threads(|| {
            RUNTIME.block_on(async move {
                let mut total_rows = 0i64;
                let mut current_batch_offset = offset;

                for batch_result in reader.by_ref() {
                    let batch = batch_result.map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                    })?;
                    let num_rows = batch.num_rows() as i64;
                    if num_rows == 0 {
                        continue;
                    }

                    let batch = normalize_batch(&batch, &self.target_schema, self.needs_conversion)
                        .map_err(|e| {
                            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                        })?;

                    let batch_buf = serialize_batch(&batch, schema_len).map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                    })?;
                    let arrow_batch =
                        ArrowRecordBatch::new().set_serialized_record_batch(batch_buf);

                    let mut append_req = writer.append(arrow_batch);
                    if let Some(off) = current_batch_offset {
                        append_req = append_req.set_offset(off);
                        current_batch_offset = Some(off + num_rows);
                    }

                    append_req.send().await.map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                    })?;

                    total_rows += num_rows;
                }

                Ok(PyAppendResult {
                    offset,
                    rows_written: total_rows,
                })
            })
        })
    }

    /// Finalizes the stream, preventing further appends. Returns the total committed row count.
    pub fn finalize(&self, py: Python<'_>) -> PyResult<i64> {
        let writer = self.writer.clone();
        py.allow_threads(|| {
            RUNTIME.block_on(async move {
                let resp = writer.finalize().await.map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                })?;
                Ok(resp.row_count)
            })
        })
    }

    /// Commits this pending stream to the BigQuery destination table atomically.
    pub fn commit(&self, py: Python<'_>) -> PyResult<()> {
        let writer = self.writer.clone();
        py.allow_threads(|| {
            RUNTIME.block_on(async move {
                writer.commit().await.map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                })?;
                Ok(())
            })
        })
    }
}

/// A Python handle for a BigQuery Committed write stream (sequential exactly-once write).
#[pyclass(name = "CommittedStream")]
pub struct PyCommittedStream {
    writer: Arc<CommittedWriter>,
    target_schema: Arc<Schema>,
    schema_len: usize,
    needs_conversion: bool,
    #[pyo3(get)]
    pub name: String,
}

#[pymethods]
impl PyCommittedStream {
    /// Writes Arrow data from a PyCapsule Arrow C Stream or DataFrame to the committed stream.
    #[pyo3(signature = (stream_capsule, offset=None))]
    pub fn write(
        &self,
        py: Python<'_>,
        stream_capsule: &Bound<'_, PyAny>,
        offset: Option<i64>,
    ) -> PyResult<PyAppendResult> {
        let mut reader = extract_reader_from_capsule(stream_capsule)?;
        let writer = self.writer.clone();
        let schema_len = self.schema_len;

        py.allow_threads(|| {
            RUNTIME.block_on(async move {
                let mut total_rows = 0i64;
                let mut current_batch_offset = offset;

                for batch_result in reader.by_ref() {
                    let batch = batch_result.map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                    })?;
                    let num_rows = batch.num_rows() as i64;
                    if num_rows == 0 {
                        continue;
                    }

                    let batch = normalize_batch(&batch, &self.target_schema, self.needs_conversion)
                        .map_err(|e| {
                            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                        })?;

                    let batch_buf = serialize_batch(&batch, schema_len).map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                    })?;
                    let arrow_batch =
                        ArrowRecordBatch::new().set_serialized_record_batch(batch_buf);

                    let mut append_req = writer.append(arrow_batch);
                    if let Some(off) = current_batch_offset {
                        append_req = append_req.set_offset(off);
                        current_batch_offset = Some(off + num_rows);
                    }

                    append_req.send().await.map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                    })?;

                    total_rows += num_rows;
                }

                Ok(PyAppendResult {
                    offset,
                    rows_written: total_rows,
                })
            })
        })
    }

    /// Finalizes the committed stream.
    pub fn finalize(&self, py: Python<'_>) -> PyResult<i64> {
        let writer = self.writer.clone();
        py.allow_threads(|| {
            RUNTIME.block_on(async move {
                let resp = writer.finalize().await.map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                })?;
                Ok(resp.row_count)
            })
        })
    }
}

/// Appends rows to the default stream (`_default`) for a table.
#[pyfunction]
#[pyo3(signature = (stream_capsule, table, project_id=None, credential_provider=None))]
pub fn write_default_stream(
    py: Python<'_>,
    stream_capsule: &Bound<'_, PyAny>,
    table: String,
    project_id: Option<String>,
    credential_provider: Option<PyObject>,
) -> PyResult<i64> {
    let mut reader = extract_reader_from_capsule(stream_capsule)?;
    let schema = reader.schema();
    let (target_schema, needs_conversion) = normalize_schema(&schema);
    let schema_buf = serialize_schema(&target_schema)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
    let schema_len = schema_buf.len();

    py.allow_threads(|| {
        RUNTIME.block_on(async move {
            let client = get_write_client(project_id, credential_provider).await?;
            let writer = client
                .arrow(ArrowSchema::new().set_serialized_schema(schema_buf))
                .default(table)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            let mut total_rows = 0i64;
            let mut writes = tokio::task::JoinSet::new();

            for batch_result in reader.by_ref() {
                let batch = batch_result.map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                })?;
                let num_rows = batch.num_rows() as i64;
                if num_rows == 0 {
                    continue;
                }
                total_rows += num_rows;

                let batch =
                    normalize_batch(&batch, &target_schema, needs_conversion).map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                    })?;

                let batch_buf = serialize_batch(&batch, schema_len).map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                })?;
                let arrow_batch = ArrowRecordBatch::new().set_serialized_record_batch(batch_buf);

                writes.spawn(writer.append(arrow_batch).send());
            }

            while let Some(res) = writes.join_next().await {
                let append_res = res.map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "Task panicked or cancelled: {e}"
                    ))
                })?;
                append_res.map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                })?;
            }

            Ok(total_rows)
        })
    })
}

/// Creates a new pending write stream for a table based on the schema of the provided Arrow C stream.
#[pyfunction]
#[pyo3(signature = (stream_capsule, table, project_id=None, credential_provider=None))]
pub fn create_pending_stream(
    py: Python<'_>,
    stream_capsule: &Bound<'_, PyAny>,
    table: String,
    project_id: Option<String>,
    credential_provider: Option<PyObject>,
) -> PyResult<PyPendingStream> {
    let reader = extract_reader_from_capsule(stream_capsule)?;
    let schema = reader.schema();
    let (target_schema, needs_conversion) = normalize_schema(&schema);
    let schema_buf = serialize_schema(&target_schema)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
    let schema_len = schema_buf.len();

    py.allow_threads(|| {
        RUNTIME.block_on(async move {
            let client = get_write_client(project_id, credential_provider).await?;
            let writer = client
                .arrow(ArrowSchema::new().set_serialized_schema(schema_buf))
                .pending(table)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Ok(PyPendingStream {
                name: writer.write_stream().to_string(),
                writer: Arc::new(writer),
                target_schema,
                schema_len,
                needs_conversion,
            })
        })
    })
}

/// Creates a new committed write stream for a table based on the schema of the provided Arrow C stream.
#[pyfunction]
#[pyo3(signature = (stream_capsule, table, project_id=None, credential_provider=None))]
pub fn create_committed_stream(
    py: Python<'_>,
    stream_capsule: &Bound<'_, PyAny>,
    table: String,
    project_id: Option<String>,
    credential_provider: Option<PyObject>,
) -> PyResult<PyCommittedStream> {
    let reader = extract_reader_from_capsule(stream_capsule)?;
    let schema = reader.schema();
    let (target_schema, needs_conversion) = normalize_schema(&schema);
    let schema_buf = serialize_schema(&target_schema)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
    let schema_len = schema_buf.len();

    py.allow_threads(|| {
        RUNTIME.block_on(async move {
            let client = get_write_client(project_id, credential_provider).await?;
            let writer = client
                .arrow(ArrowSchema::new().set_serialized_schema(schema_buf))
                .committed(table)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Ok(PyCommittedStream {
                name: writer.write_stream().to_string(),
                writer: Arc::new(writer),
                target_schema,
                schema_len,
                needs_conversion,
            })
        })
    })
}

/// Commits one or more pending write streams to their destination table atomically.
#[pyfunction]
#[pyo3(signature = (table, stream_names, project_id=None, credential_provider=None))]
pub fn batch_commit_streams(
    py: Python<'_>,
    table: String,
    stream_names: Vec<String>,
    project_id: Option<String>,
    credential_provider: Option<PyObject>,
) -> PyResult<()> {
    py.allow_threads(|| {
        RUNTIME.block_on(async move {
            let client = get_write_client(project_id, credential_provider).await?;
            client
                .batch_commit(table, stream_names)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(())
        })
    })
}

/// Registers write module functions and types on the Python extension module.
pub fn register_write_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyAppendResult>()?;
    m.add_class::<PyPendingStream>()?;
    m.add_class::<PyCommittedStream>()?;
    m.add_function(wrap_pyfunction!(write_default_stream, m)?)?;
    m.add_function(wrap_pyfunction!(create_pending_stream, m)?)?;
    m.add_function(wrap_pyfunction!(create_committed_stream, m)?)?;
    m.add_function(wrap_pyfunction!(batch_commit_streams, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn test_serialize_schema_and_batch() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        let schema_buf = serialize_schema(&schema).expect("serialize schema");
        assert!(!schema_buf.is_empty());
        let schema_len = schema_buf.len();

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["alice", "bob", "charlie"])),
            ],
        )
        .expect("create batch");

        let batch_buf = serialize_batch(&batch, schema_len).expect("serialize batch");
        assert!(!batch_buf.is_empty());
    }

    #[test]
    fn test_append_result_representation() {
        let res = PyAppendResult {
            offset: Some(100),
            rows_written: 50,
        };
        assert_eq!(
            res.__repr__(),
            "AppendResult(offset=Some(100), rows_written=50)"
        );

        let res_no_offset = PyAppendResult {
            offset: None,
            rows_written: 50,
        };
        assert_eq!(
            res_no_offset.__repr__(),
            "AppendResult(offset=None, rows_written=50)"
        );
    }
}
