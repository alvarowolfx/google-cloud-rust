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

"""Example demonstrating Polars BigQuery Write Plugin with offset controls and stream types."""

import os
import polars as pl
import polars_bigquery as pl_bq


def main():
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT", "my-project")
    dataset_id = "test_dataset"
    table_id = "users_ingest"
    table = f"{project_id}.{dataset_id}.{table_id}"

    print("=" * 80)
    print("Polars BigQuery Write Plugin (Storage Write API + Offset Controls)")
    print("=" * 80)

    # Sample DataFrame
    df = pl.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "score": [95.5, 88.0, 92.25, 78.5, 89.0],
    })

    print("\nData to ingest:")
    print(df)

    # --------------------------------------------------------------------------
    # Pattern 1: High-throughput Default Stream Write (No offset tracking)
    # --------------------------------------------------------------------------
    print("\n--- Pattern 1: Fast default stream append ---")
    print(">>> rows = pl_bq.write_bigquery(df, table, stream_type='default')")
    print(f"Target: {table}")
    print("Characteristics: Lowest latency, at-least-once ingestion, no commit required.")

    # --------------------------------------------------------------------------
    # Pattern 2: ACID Transaction with Pending Stream (Auto-managed commit)
    # --------------------------------------------------------------------------
    print("\n--- Pattern 2: Atomic Transaction (Pending Stream) ---")
    print("""
with pl_bq.WriteTransaction(table) as txn:
    # All writes within this block commit atomically upon clean exit.
    # If any error occurs, the transaction aborts with zero partial rows in BQ.
    txn.write(df.slice(0, 3))
    txn.write(df.slice(3, 2))
""")

    # --------------------------------------------------------------------------
    # Pattern 3: Fine-Grained Stream & Explicit Offset Control
    # --------------------------------------------------------------------------
    print("\n--- Pattern 3: Explicit Offset Control for Exactly-Once Semantics (EOS) ---")
    print("""
client = pl_bq.WriteClient(project_id=project_id)
stream = client.create_pending_stream(table, sample_data=df)

# Customers manage and track their offsets across batches:
offset = 0

# Batch 1 with explicit starting offset 0
res1 = stream.write(df.slice(0, 3), offset=offset)
offset += res1.rows_written
print(f"Batch 1 written ({res1.rows_written} rows). Next customer offset: {offset}")

# Batch 2 with explicit offset 3
res2 = stream.write(df.slice(3, 2), offset=offset)
offset += res2.rows_written
print(f"Batch 2 written ({res2.rows_written} rows). Next customer offset: {offset}")

# Idempotent Retry Demonstration:
# If a network timeout occurs, re-sending with offset=3 deduplicates automatically on BQ!
# stream.write(df.slice(3, 2), offset=3)

# Finalize and atomically commit
stream.finalize()
client.batch_commit(table, [stream.name])
""")

    # --------------------------------------------------------------------------
    # Pattern 4: DataFrame Native Extension
    # --------------------------------------------------------------------------
    print("\n--- Pattern 4: Native Polars DataFrame Extension ---")
    print(f">>> df.write_bigquery('{table}', stream_type='default')")
    print("=" * 80)


if __name__ == "__main__":
    main()
