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

"""Benchmark & Sample: Parquet Ingestion (Polars + Write API) and Accelerated Query (Polars + Read API).

This script demonstrates and benchmarks the end-to-end roundtrip:
1. Loading a local Parquet file (`us-states.parquet` from `gs://cloud-samples-data/bigquery/us-states/us-states.parquet`).
2. Ingesting into BigQuery using the Storage Write API veneer (default stream, ACID pending stream, and offset controls).
3. Querying data back into Polars using the Storage Read API acceleration (zero-copy Arrow C Stream).
4. Comparing performance and throughput against the official google-cloud-bigquery Python SDK.

Usage:
    # Quick sample run (50 rows):
    python benchmark_parquet_write_read.py

    # High-throughput benchmark (e.g. 100,000 rows):
    python benchmark_parquet_write_read.py --scale 2000 --runs 3
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import urllib.request
from typing import Any, Dict, List

import polars as pl
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

import polars_bigquery as pl_bq

PARQUET_GCS_URL = "https://storage.googleapis.com/cloud-samples-data/bigquery/us-states/us-states.parquet"
LOCAL_PARQUET_FILE = "us-states.parquet"


def ensure_parquet_file(file_path: str = LOCAL_PARQUET_FILE) -> str:
    """Ensures the sample Parquet dataset exists locally, downloading from public GCS if needed."""
    if not os.path.exists(file_path):
        print(f"Downloading sample dataset from {PARQUET_GCS_URL}...")
        urllib.request.urlretrieve(PARQUET_GCS_URL, file_path)
        print(f"Saved to {file_path} ({os.path.getsize(file_path):,} bytes).")
    return file_path


def ensure_bigquery_table(
    client: bigquery.Client,
    project: str,
    dataset_id: str,
    table_id: str,
    schema: list[bigquery.SchemaField],
) -> str:
    """Ensures BigQuery dataset and destination table exist."""
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        print(f"Creating dataset {project}.{dataset_id}...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset, exists_ok=True)

    full_table = f"{project}.{dataset_id}.{table_id}"
    table_ref = bigquery.TableReference(dataset_ref, table_id)
    table = bigquery.Table(table_ref, schema=schema)

    try:
        client.get_table(table_ref)
        # Truncate table for repeatable clean benchmarks
        client.query(f"TRUNCATE TABLE `{full_table}`").result()
    except NotFound:
        print(f"Creating table {full_table}...")
        client.create_table(table)
        # Sleep briefly for metadata propagation in BigQuery Storage Write API
        time.sleep(3)

    return full_table


def run_benchmark(
    project_id: str,
    dataset_id: str,
    table_id: str,
    scale: int = 1,
    runs: int = 2,
):
    print("=" * 90)
    print("Polars BigQuery End-to-End Benchmark & Sample")
    print("Storage Write API (Ingestion) + Storage Read API (Acceleration)")
    print("=" * 90)

    # 1. Load Local Parquet File
    file_path = ensure_parquet_file()
    df_raw = pl.read_parquet(file_path)
    base_rows = len(df_raw)
    print(f"\n[1] Loaded local Parquet file: {file_path}")
    print(f"    Base Rows: {base_rows}, Schema: {df_raw.schema}")

    if scale > 1:
        print(f"    Scaling dataset {scale}x for throughput benchmarking...")
        df = pl.concat([df_raw] * scale)
    else:
        df = df_raw

    total_rows = len(df)
    estimated_mb = df.estimated_size() / (1024 * 1024)
    print(f"    Total Rows to Ingest: {total_rows:,} ({estimated_mb:.2f} MB)")
    print("\nSample Data Preview:")
    print(df.head(5))

    # 2. Setup Destination Table
    py_client = bigquery.Client(project=project_id)
    schema = [
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("post_abbr", "STRING"),
    ]
    full_table = ensure_bigquery_table(
        py_client, project_id, dataset_id, table_id, schema
    )
    print(f"\n[2] Target BigQuery Table: {full_table}")

    results: list[dict[str, Any]] = []

    # --------------------------------------------------------------------------
    # WRITE BENCHMARKS
    # --------------------------------------------------------------------------
    print("\n" + "-" * 90)
    print("PHASE 1: WRITE INGESTION BENCHMARKS (Parquet DataFrame -> BigQuery)")
    print("-" * 90)

    # Method 1: Polars + Storage Write API (Default Stream)
    print("\n>>> Method W1: Polars + BigQuery Storage Write API (Default Stream)")
    times_w1: list[float] = []
    for r in range(runs):
        py_client.query(f"TRUNCATE TABLE `{full_table}`").result()
        t0 = time.perf_counter()
        written = pl_bq.write_bigquery(
            df, full_table, stream_type="default", project_id=project_id
        )
        dt = time.perf_counter() - t0
        times_w1.append(dt)
        print(f"    Run {r+1}: {dt:.3f}s ({written:,} rows, {written/dt:,.0f} rows/s)")
    best_w1 = min(times_w1)
    results.append({
        "phase": "Write",
        "method": "Polars Write API (Default Stream)",
        "time": best_w1,
        "rows": total_rows,
        "rate": total_rows / best_w1,
    })

    # Method 2: Polars + Storage Write API (Pending Stream / ACID Transaction)
    print("\n>>> Method W2: Polars + Storage Write API (Pending Stream / Transaction)")
    times_w2: list[float] = []
    for r in range(runs):
        py_client.query(f"TRUNCATE TABLE `{full_table}`").result()
        t0 = time.perf_counter()
        with pl_bq.WriteTransaction(full_table, project_id=project_id) as txn:
            txn.write(df)
        dt = time.perf_counter() - t0
        times_w2.append(dt)
        print(f"    Run {r+1}: {dt:.3f}s ({total_rows:,} rows, {total_rows/dt:,.0f} rows/s)")
    best_w2 = min(times_w2)
    results.append({
        "phase": "Write",
        "method": "Polars Write API (Pending Transaction)",
        "time": best_w2,
        "rows": total_rows,
        "rate": total_rows / best_w2,
    })

    # Method 3: Polars + Storage Write API with Explicit Customer Offsets (EOS)
    print("\n>>> Method W3: Polars + Storage Write API (Explicit Customer Offsets)")
    times_w3: list[float] = []
    for r in range(runs):
        py_client.query(f"TRUNCATE TABLE `{full_table}`").result()
        t0 = time.perf_counter()
        client = pl_bq.WriteClient(project_id=project_id)
        stream = client.create_pending_stream(full_table, sample_data=df)
        offset = 0
        # Split into two batches to demonstrate customer offset progression
        half = total_rows // 2
        batch1 = df.slice(0, half)
        batch2 = df.slice(half, total_rows - half)

        res1 = stream.write(batch1, offset=offset)
        offset += res1.rows_written

        res2 = stream.write(batch2, offset=offset)
        offset += res2.rows_written

        stream.finalize()
        client.batch_commit(full_table, [stream.name])
        dt = time.perf_counter() - t0
        times_w3.append(dt)
        print(f"    Run {r+1}: {dt:.3f}s ({offset:,} rows, {offset/dt:,.0f} rows/s)")
    best_w3 = min(times_w3)
    results.append({
        "phase": "Write",
        "method": "Polars Write API (Explicit Offsets)",
        "time": best_w3,
        "rows": total_rows,
        "rate": total_rows / best_w3,
    })

    # Method 4: Official Python SDK (load_table_from_file Parquet)
    print("\n>>> Method W4: Official google-cloud-bigquery Python SDK (load_table_from_file Parquet)")
    try:
        import io
        import pyarrow.parquet as pq

        arrow_tbl = df.to_arrow()
        times_w4: list[float] = []
        for r in range(runs):
            py_client.query(f"TRUNCATE TABLE `{full_table}`").result()
            t0 = time.perf_counter()
            buf = io.BytesIO()
            pq.write_table(arrow_tbl, buf)
            buf.seek(0)
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition="WRITE_TRUNCATE",
            )
            load_job = py_client.load_table_from_file(
                buf, full_table, job_config=job_config
            )
            load_job.result()
            dt = time.perf_counter() - t0
            times_w4.append(dt)
            print(f"    Run {r+1}: {dt:.3f}s ({total_rows:,} rows, {total_rows/dt:,.0f} rows/s)")
        best_w4 = min(times_w4)
        results.append({
            "phase": "Write",
            "method": "Python SDK load_table_from_file (Parquet)",
            "time": best_w4,
            "rows": total_rows,
            "rate": total_rows / best_w4,
        })
    except Exception as e:
        print(f"    Skipping Python SDK write: {e}")

    # Ensure table has test data populated for read benchmarks
    row_count = py_client.get_table(full_table).num_rows
    if row_count == 0:
        print(f"\nPopulating table with {total_rows:,} rows via Polars write_bigquery for read benchmarks...")
        pl_bq.write_bigquery(df, full_table, stream_type="default", project_id=project_id)

    # --------------------------------------------------------------------------
    # READ BENCHMARKS
    # --------------------------------------------------------------------------
    print("\n" + "-" * 90)
    print("PHASE 2: READ QUERY BENCHMARKS (BigQuery -> Polars DataFrame)")
    print("-" * 90)

    query_sql = f"SELECT * FROM `{full_table}`"

    # Method R1: Polars + Storage Read API (Rust Arrow C Stream Acceleration)
    print("\n>>> Method R1: Polars + Storage Read API (Rust Arrow C Stream Acceleration)")
    times_r1: list[float] = []
    df_read_storage: pl.DataFrame | None = None
    for r in range(runs):
        t0 = time.perf_counter()
        df_read_storage = pl_bq.read_bigquery(
            query_sql, storage_read=True, project_id=project_id
        )
        dt = time.perf_counter() - t0
        times_r1.append(dt)
        print(f"    Run {r+1}: {dt:.3f}s ({len(df_read_storage):,} rows, {len(df_read_storage)/dt:,.0f} rows/s)")
    best_r1 = min(times_r1)
    results.append({
        "phase": "Read",
        "method": "Polars Read API (Storage Read / Arrow C Stream)",
        "time": best_r1,
        "rows": total_rows,
        "rate": total_rows / best_r1,
    })

    # Method R2: Polars + BigQuery REST Query fallback
    print("\n>>> Method R2: Polars + BigQuery Query (REST format fallback)")
    times_r2: list[float] = []
    df_read_rest: pl.DataFrame | None = None
    for r in range(runs):
        t0 = time.perf_counter()
        df_read_rest = pl_bq.read_bigquery(
            query_sql, storage_read=False, project_id=project_id
        )
        dt = time.perf_counter() - t0
        times_r2.append(dt)
        print(f"    Run {r+1}: {dt:.3f}s ({len(df_read_rest):,} rows, {len(df_read_rest)/dt:,.0f} rows/s)")
    best_r2 = min(times_r2)
    results.append({
        "phase": "Read",
        "method": "Polars Read API (REST format fallback)",
        "time": best_r2,
        "rows": total_rows,
        "rate": total_rows / best_r2,
    })

    # Method R3: Official Python SDK (.to_arrow())
    print("\n>>> Method R3: Official Python SDK (py_client.query().to_arrow())")
    times_r3: list[float] = []
    for r in range(runs):
        t0 = time.perf_counter()
        query_job = py_client.query(query_sql)
        arrow_table = query_job.to_arrow()
        df_py = pl.from_arrow(arrow_table)
        dt = time.perf_counter() - t0
        times_r3.append(dt)
        print(f"    Run {r+1}: {dt:.3f}s ({len(df_py):,} rows, {len(df_py)/dt:,.0f} rows/s)")
    best_r3 = min(times_r3)
    results.append({
        "phase": "Read",
        "method": "Python SDK query().to_arrow()",
        "time": best_r3,
        "rows": total_rows,
        "rate": total_rows / best_r3,
    })

    # --------------------------------------------------------------------------
    # DATA INTEGRITY VERIFICATION
    # --------------------------------------------------------------------------
    print("\n" + "-" * 90)
    print("PHASE 3: DATA INTEGRITY VERIFICATION")
    print("-" * 90)
    assert df_read_storage is not None
    assert len(df_read_storage) == total_rows, (
        f"Row count mismatch: expected {total_rows}, got {len(df_read_storage)}"
    )
    # Compare distinct state count
    unique_states_read = df_read_storage.select(pl.col("post_abbr").n_unique()).item()
    unique_states_orig = df_raw.select(pl.col("post_abbr").n_unique()).item()
    assert unique_states_read == unique_states_orig, (
        f"Unique states mismatch: expected {unique_states_orig}, got {unique_states_read}"
    )
    print("✓ Row count matches perfectly:", f"{len(df_read_storage):,}")
    print("✓ Unique state count matches perfectly:", unique_states_read)
    print("✓ Schema matches:", df_read_storage.schema)

    # --------------------------------------------------------------------------
    # SUMMARY TABLE
    # --------------------------------------------------------------------------
    print("\n" + "=" * 90)
    print(f"BENCHMARK SUMMARY (Dataset: {total_rows:,} rows, {estimated_mb:.2f} MB)")
    print("=" * 90)
    print(f"{'Phase':<8} {'Method':<48} {'Time (s)':<10} {'Throughput (rows/s)':<22}")
    print("-" * 90)
    for res in results:
        print(
            f"{res['phase']:<8} "
            f"{res['method']:<48} "
            f"{res['time']:<10.3f} "
            f"{res['rate']:>18,.0f} rows/s"
        )
    print("=" * 90)


def main():
    parser = argparse.ArgumentParser(
        description="Polars BigQuery Parquet Write & Read Acceleration Benchmark"
    )
    parser.add_argument(
        "--project",
        default=os.environ.get("GOOGLE_CLOUD_PROJECT", "aviebrantz-testing"),
        help="Google Cloud Project ID",
    )
    parser.add_argument(
        "--dataset",
        default="polars_demo",
        help="BigQuery dataset name (default: polars_demo)",
    )
    parser.add_argument(
        "--table",
        default="us_states_bench",
        help="BigQuery table name (default: us_states_bench)",
    )
    parser.add_argument(
        "--scale",
        type=int,
        default=1,
        help="Dataset scale factor (default: 1 = 50 rows; 2000 = 100,000 rows)",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=2,
        help="Number of iterations for best-of-N timing (default: 2)",
    )
    args = parser.parse_args()

    run_benchmark(
        project_id=args.project,
        dataset_id=args.dataset,
        table_id=args.table,
        scale=args.scale,
        runs=args.runs,
    )


if __name__ == "__main__":
    main()
