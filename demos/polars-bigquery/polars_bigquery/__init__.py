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


__all__ = ["scan_bigquery", "read_bigquery"]
