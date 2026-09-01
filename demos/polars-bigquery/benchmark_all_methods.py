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

"""Comprehensive Benchmark: Python SDK vs Rust REST vs Rust Storage/Arrow C Stream."""

import os
import time
import polars as pl
import polars_bigquery as pbq
from google.cloud import bigquery
from google.cloud import bigquery_storage

WARMUP_RUNS = 1
BENCHMARK_RUNS = 2

def run_benchmarks():
    project = pbq._resolve_project_id(None)
    print(f"Using Project: {project}")
    py_bq = bigquery.Client(project=project)
    py_storage = bigquery_storage.BigQueryReadClient()

    test_datasets = [
        ("Small (1,000 rows)", """
            SELECT x AS id, CONCAT('user_', CAST(x AS STRING)) AS username, CAST(x AS FLOAT64) * 1.5 AS balance
            FROM UNNEST(GENERATE_ARRAY(1, 1000)) AS x
        """, 1_000),
        ("Medium (10,000 rows)", """
            SELECT x AS id, CONCAT('user_', CAST(x AS STRING)) AS username, CAST(x AS FLOAT64) * 1.5 AS balance
            FROM UNNEST(GENERATE_ARRAY(1, 10000)) AS x
        """, 10_000),
        ("Large (200,000 rows)", """
            SELECT x AS id, CONCAT('user_', CAST(x AS STRING)) AS username, CAST(x AS FLOAT64) * 1.5 AS balance
            FROM UNNEST(GENERATE_ARRAY(1, 200000)) AS x
        """, 200_000),
        ("Public USA Names (500,000 rows)", """
            SELECT name, gender, state, year, number
            FROM `bigquery-public-data.usa_names.usa_1910_current`
            LIMIT 500000
        """, 500_000),
        ("Public USA Names (2,000,000 rows)", """
            SELECT name, gender, state, year, number
            FROM `bigquery-public-data.usa_names.usa_1910_current`
            LIMIT 2000000
        """, 2_000_000),
        ("Public USA Names (5,000,000 rows - Multi-Stream)", """
            SELECT name, gender, state, year, number
            FROM `bigquery-public-data.usa_names.usa_1910_current`
            LIMIT 5000000
        """, 5_000_000),
    ]

    for title, query, expected_rows in test_datasets:
        print("\n" + "=" * 100)
        print(f"BENCHMARK: {title}")
        print("=" * 100)

        results = {}

        # 1. Python SDK REST -> Polars DataFrame
        def run_py_rest():
            job = py_bq.query(query)
            table = job.to_arrow(create_bqstorage_client=False)
            df = pl.from_arrow(table)
            return len(df)

        # 2. Python SDK Storage API -> Polars DataFrame
        def run_py_storage():
            job = py_bq.query(query)
            table = job.to_arrow(bqstorage_client=py_storage, create_bqstorage_client=True)
            df = pl.from_arrow(table)
            return len(df)

        # 3. Polars Eager (Rust as I/O backend)
        def run_polars_eager():
            df = pbq.read_bigquery(query, project_id=project)
            return len(df)

        # 4. Polars Lazy scan (Rust as I/O backend)
        def run_polars_lazy():
            lf = pbq.scan_bigquery(query, project_id=project)
            df = lf.collect()
            return len(df)

        runners = [
            ("1. Python SDK REST -> Polars", run_py_rest),
            ("2. Python SDK Storage API -> Polars", run_py_storage),
            ("3. Polars I/O Plugin (Eager)", run_polars_eager),
            ("4. Polars I/O Plugin (Lazy Scan)", run_polars_lazy),
        ]

        # Warmup
        for name, fn in runners:
            if expected_rows >= 2_000_000 and "REST" in name:
                continue
            try:
                fn()
            except Exception as e:
                print(f"  Warmup error for {name}: {e}")

        # Timed runs
        for name, fn in runners:
            if expected_rows > 2_000_000 and "REST" in name:
                continue
            times = []
            row_count = 0
            runs = 1 if (expected_rows >= 2_000_000 and "REST" in name) else BENCHMARK_RUNS
            for r in range(runs):
                t0 = time.perf_counter()
                row_count = fn()
                elapsed = time.perf_counter() - t0
                times.append(elapsed)
            best_time = min(times)
            throughput = row_count / best_time if best_time > 0 else 0
            results[name] = (best_time, throughput, row_count)
            print(f"  {name:<40} Best: {best_time:6.3f}s | {throughput:>10,.0f} rows/s")

        # Summary table
        baseline_time = results.get("1. Python SDK REST -> Polars", (0, 0, 0))[0]
        if baseline_time == 0 and "2. Python SDK Storage API -> Polars" in results:
            baseline_time = results["2. Python SDK Storage API -> Polars"][0]
        print("\n" + "-" * 100)
        print(f"{'Method':<42} | {'Latency':<9} | {'Throughput':<14} | {'Speedup':<18}")
        print("-" * 100)
        for name, (best_time, throughput, _) in results.items():
            speedup = baseline_time / best_time if best_time > 0 else 1.0
            print(f"{name:<42} | {best_time:6.3f}s  | {throughput:>10,.0f} r/s  | {speedup:6.2f}x")
        print("-" * 100)

if __name__ == "__main__":
    run_benchmarks()
