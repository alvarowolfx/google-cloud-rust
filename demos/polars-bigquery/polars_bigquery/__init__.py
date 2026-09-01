# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""BigQuery I/O Plugin for Polars using Arrow C Stream FFI and Storage Read API acceleration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Dict, Iterator, Optional, Sequence

import polars as pl
from polars.io.plugins import register_io_source

try:
    from . import _polars_bigquery
except ImportError:
    import _polars_bigquery  # type: ignore

if TYPE_CHECKING:
    from polars._typing import SchemaDict


import functools
import os

@functools.lru_cache(maxsize=1)
def _get_default_adc_project() -> str | None:
    proj = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT")
    if proj:
        return proj
    try:
        import google.auth

        _, default_proj = google.auth.default()
        return default_proj
    except Exception:
        return None


def _resolve_project_id(project_id: str | None) -> str | None:
    """Resolves project ID from argument, environment variables, or Google ADC."""
    if project_id is not None:
        return project_id
    return _get_default_adc_project()


def _bigquery_source_generator(
    with_columns: Sequence[str] | None,
    predicate: pl.Expr | None,
    n_rows: int | None,
    batch_size: int | None,
    query: str,
    project_id: str | None,
    credential_provider: Any | None,
    storage_read: bool,
) -> Iterator[pl.DataFrame]:
    """Internal generator that reads chunks from BigQuery Arrow C Stream into Polars."""
    resolved_proj = _resolve_project_id(project_id)
    stream_capsule = _polars_bigquery.execute_bigquery_stream(
        query=query,
        project_id=resolved_proj,
        credential_provider=credential_provider,
        storage_read=storage_read,
    )

    # Lazily stream batches from the Arrow C Stream PyCapsule
    lf = pl.scan_arrow_c_stream(stream_capsule)

    if with_columns is not None:
        lf = lf.select(with_columns)

    if predicate is not None:
        lf = lf.filter(predicate)

    if n_rows is not None:
        lf = lf.limit(n_rows)

    df = lf.collect()
    yield df


def scan_bigquery(
    query: str,
    *,
    schema: SchemaDict | None = None,
    project_id: str | None = None,
    credential_provider: Any | None = None,
    storage_read: bool = True,
) -> pl.LazyFrame:
    """Lazily scan query results from BigQuery into a Polars LazyFrame.

    Uses high-speed gRPC BigQuery Storage Read API acceleration and
    standard Arrow C Stream zero-copy FFI interchange.

    Parameters
    ----------
    query : str
        The SQL query to execute in BigQuery.
    schema : SchemaDict, optional
        Optional expected schema dictionary mapping column names to Polars DataTypes.
        If omitted, schema is discovered upon query start.
    project_id : str, optional
        Google Cloud Project ID for query billing and execution.
        If omitted, discovered via Application Default Credentials (ADC).
    credential_provider : CredentialProvider, optional
        A Polars credential provider such as `pl.CredentialProviderGCP()`
        or a custom token callable. If omitted, uses default ADC.
    storage_read : bool, default True
        Whether to accelerate reads using the BigQuery Storage Read API.

    Returns
    -------
    pl.LazyFrame
        A LazyFrame representing the streaming query results.

    Example
    -------
    >>> import polars as pl
    >>> import polars_bigquery as pl_bq
    >>> 
    >>> # Using standard Application Default Credentials (ADC)
    >>> lf = pl_bq.scan_bigquery("SELECT name, score FROM `my_dataset.scores` WHERE score > 50")
    >>> df = lf.filter(pl.col("score") > 80).collect()
    >>>
    >>> # Using Polars CredentialProviderGCP
    >>> lf = pl_bq.scan_bigquery(
    ...     "SELECT * FROM `my_dataset.users`",
    ...     credential_provider=pl.CredentialProviderGCP(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    ... )
    >>> print(lf.collect())
    """
    if schema is not None:
        return register_io_source(
            io_source=lambda with_cols, pred, n_rows, b_size: _bigquery_source_generator(
                with_cols,
                pred,
                n_rows,
                b_size,
                query=query,
                project_id=project_id,
                credential_provider=credential_provider,
                storage_read=storage_read,
            ),
            schema=schema,
        )

    # If schema is not pre-specified, directly scan the Arrow C Stream capsule
    resolved_proj = _resolve_project_id(project_id)
    stream_capsule = _polars_bigquery.execute_bigquery_stream(
        query=query,
        project_id=resolved_proj,
        credential_provider=credential_provider,
        storage_read=storage_read,
    )
    return pl.scan_arrow_c_stream(stream_capsule)


def read_bigquery(
    query: str,
    *,
    project_id: str | None = None,
    credential_provider: Any | None = None,
    storage_read: bool = True,
) -> pl.DataFrame:
    """Eagerly execute a SQL query in BigQuery and read results into a Polars DataFrame.

    Parameters
    ----------
    query : str
        The SQL query to execute in BigQuery.
    project_id : str, optional
        Google Cloud Project ID.
    credential_provider : CredentialProvider, optional
        A Polars credential provider such as `pl.CredentialProviderGCP()`.
    storage_read : bool, default True
        Whether to accelerate reads using the BigQuery Storage Read API.

    Returns
    -------
    pl.DataFrame
        The materialized query results as a DataFrame.
    """
    resolved_proj = _resolve_project_id(project_id)
    stream_capsule = _polars_bigquery.execute_bigquery_stream(
        query=query,
        project_id=resolved_proj,
        credential_provider=credential_provider,
        storage_read=storage_read,
    )
    return pl.scan_arrow_c_stream(stream_capsule).collect()


AppendResult = getattr(_polars_bigquery, "AppendResult", None)
PendingStream = getattr(_polars_bigquery, "PendingStream", None)
CommittedStream = getattr(_polars_bigquery, "CommittedStream", None)


def _normalize_table_name(table: str, project_id: str | None = None) -> str:
    """Normalizes dataset.table or project.dataset.table to standard resource URI."""
    table = table.strip().strip("`")
    if table.startswith("projects/") and "/datasets/" in table and "/tables/" in table:
        return table
    parts = table.split(".")
    if len(parts) == 3:
        return f"projects/{parts[0]}/datasets/{parts[1]}/tables/{parts[2]}"
    elif len(parts) == 2:
        proj = _resolve_project_id(project_id)
        if not proj:
            raise ValueError(
                f"Cannot resolve project ID for table '{table}'. "
                f"Please supply `project_id` or set GOOGLE_CLOUD_PROJECT."
            )
        return f"projects/{proj}/datasets/{parts[0]}/tables/{parts[1]}"
    else:
        raise ValueError(
            f"Invalid BigQuery table name format: '{table}'. "
            f"Expected 'dataset.table', 'project.dataset.table', or 'projects/.../datasets/.../tables/...'"
        )


def _extract_arrow_stream_capsule(data: Any) -> Any:
    """Extracts Arrow C Stream PyCapsule from a Polars DataFrame or Arrow-compatible object."""
    if hasattr(data, "__arrow_c_stream__"):
        return data.__arrow_c_stream__()
    if hasattr(data, "to_arrow"):
        arrow_obj = data.to_arrow()
        if hasattr(arrow_obj, "__arrow_c_stream__"):
            return arrow_obj.__arrow_c_stream__()
    if isinstance(data, pl.LazyFrame):
        return data.collect().__arrow_c_stream__()
    raise TypeError(
        f"Expected pl.DataFrame, pl.LazyFrame, or Arrow C Stream-compatible object, got {type(data)}"
    )


def write_bigquery(
    data: pl.DataFrame | pl.LazyFrame | Any,
    table: str,
    *,
    stream_type: str = "default",
    offset: int | None = None,
    project_id: str | None = None,
    credential_provider: Any | None = None,
) -> int:
    """Ingests a Polars DataFrame or LazyFrame into a BigQuery table using the Storage Write API.

    Parameters
    ----------
    data : pl.DataFrame or pl.LazyFrame
        The data to ingest into BigQuery.
    table : str
        Target table in 'dataset.table', 'project.dataset.table', or full resource path format.
    stream_type : {"default", "pending", "committed"}, default "default"
        The BigQuery write stream type to use:
        - "default": High-throughput, at-least-once ingestion without explicit commit step.
        - "pending": Exactly-once transactional write that commits atomically upon completion.
        - "committed": Sequential exactly-once write with immediate commit per batch.
    offset : int, optional
        Starting row offset. If None, offset verification is omitted (appends to end of stream).
        If specified, guarantees idempotent retries and validates sequence alignment.
    project_id : str, optional
        Google Cloud Project ID for query billing and execution.
    credential_provider : CredentialProvider, optional
        A Polars credential provider such as `pl.CredentialProviderGCP()`.

    Returns
    -------
    int
        Total number of rows successfully written.
    """
    resolved_proj = _resolve_project_id(project_id)
    norm_table = _normalize_table_name(table, resolved_proj)

    capsule = _extract_arrow_stream_capsule(data)

    if stream_type == "default":
        if offset is not None:
            raise ValueError(
                "BigQuery default stream does not support explicit offsets. "
                "Use stream_type='pending' or stream_type='committed' for offset controls."
            )
        return _polars_bigquery.write_default_stream(
            stream_capsule=capsule,
            table=norm_table,
            project_id=resolved_proj,
            credential_provider=credential_provider,
        )
    elif stream_type == "pending":
        stream = _polars_bigquery.create_pending_stream(
            stream_capsule=capsule,
            table=norm_table,
            project_id=resolved_proj,
            credential_provider=credential_provider,
        )
        res = stream.write(capsule, offset=offset)
        stream.finalize()
        stream.commit()
        return res.rows_written
    elif stream_type == "committed":
        stream = _polars_bigquery.create_committed_stream(
            stream_capsule=capsule,
            table=norm_table,
            project_id=resolved_proj,
            credential_provider=credential_provider,
        )
        res = stream.write(capsule, offset=offset)
        stream.finalize()
        return res.rows_written
    else:
        raise ValueError(
            f"Invalid stream_type '{stream_type}'. Expected 'default', 'pending', or 'committed'."
        )


class WriteClient:
    """Client for fine-grained BigQuery Storage Write API stream lifecycle and offset controls."""

    def __init__(
        self,
        *,
        project_id: str | None = None,
        credential_provider: Any | None = None,
    ) -> None:
        self.project_id = _resolve_project_id(project_id)
        self.credential_provider = credential_provider

    def create_pending_stream(self, table: str, sample_data: Any) -> Any:
        """Creates a new pending write stream for the table based on sample data schema."""
        norm_table = _normalize_table_name(table, self.project_id)
        capsule = _extract_arrow_stream_capsule(sample_data)
        return _polars_bigquery.create_pending_stream(
            stream_capsule=capsule,
            table=norm_table,
            project_id=self.project_id,
            credential_provider=self.credential_provider,
        )

    def create_committed_stream(self, table: str, sample_data: Any) -> Any:
        """Creates a new committed write stream for the table based on sample data schema."""
        norm_table = _normalize_table_name(table, self.project_id)
        capsule = _extract_arrow_stream_capsule(sample_data)
        return _polars_bigquery.create_committed_stream(
            stream_capsule=capsule,
            table=norm_table,
            project_id=self.project_id,
            credential_provider=self.credential_provider,
        )

    def batch_commit(
        self, table: str, streams: Sequence[str | Any]
    ) -> None:
        """Atomically commits a batch of pending streams to the destination table."""
        norm_table = _normalize_table_name(table, self.project_id)
        stream_names = [
            s.name if hasattr(s, "name") else str(s) for s in streams
        ]
        _polars_bigquery.batch_commit_streams(
            table=norm_table,
            stream_names=stream_names,
            project_id=self.project_id,
            credential_provider=self.credential_provider,
        )


class WriteTransaction:
    """ACID transactional context manager for BigQuery writes.

    Creates a pending stream, allows streaming appends with or without offset controls,
    and automatically finalizes and commits all streams upon successful exit of the block.
    If an exception occurs within the block, the transaction aborts without committing.
    """

    def __init__(
        self,
        table: str,
        *,
        project_id: str | None = None,
        credential_provider: Any | None = None,
    ) -> None:
        self.table = table
        self.project_id = _resolve_project_id(project_id)
        self.credential_provider = credential_provider
        self.client = WriteClient(
            project_id=self.project_id,
            credential_provider=self.credential_provider,
        )
        self.streams: list[Any] = []
        self._default_stream: Any = None

    def __enter__(self) -> "WriteTransaction":
        return self

    def write(self, data: Any, *, offset: int | None = None) -> Any:
        """Appends data to the transaction's primary stream."""
        capsule = _extract_arrow_stream_capsule(data)
        if self._default_stream is None:
            self._default_stream = self.client.create_pending_stream(
                self.table, data
            )
            self.streams.append(self._default_stream)
        return self._default_stream.write(capsule, offset=offset)

    def create_stream(self, sample_data: Any) -> Any:
        """Creates an additional concurrent pending stream for parallel workers."""
        stream = self.client.create_pending_stream(self.table, sample_data)
        self.streams.append(stream)
        return stream

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is not None:
            # Transaction failed: abort without committing
            return
        # Finalize and batch commit all streams atomically
        for s in self.streams:
            s.finalize()
        if self.streams:
            self.client.batch_commit(self.table, self.streams)


def _register_dataframe_extensions() -> None:
    if not hasattr(pl.DataFrame, "write_bigquery"):
        setattr(
            pl.DataFrame,
            "write_bigquery",
            lambda self, table, **kwargs: write_bigquery(self, table, **kwargs),
        )
    if not hasattr(pl.LazyFrame, "sink_bigquery"):
        setattr(
            pl.LazyFrame,
            "sink_bigquery",
            lambda self, table, **kwargs: write_bigquery(self, table, **kwargs),
        )


_register_dataframe_extensions()

__all__ = [
    "scan_bigquery",
    "read_bigquery",
    "write_bigquery",
    "WriteClient",
    "WriteTransaction",
    "AppendResult",
    "PendingStream",
    "CommittedStream",
]
