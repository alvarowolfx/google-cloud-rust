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

"""Example demonstrating Polars I/O integration with BigQuery and CredentialProviderGCP."""

import os
import polars as pl
from polars_bigquery import scan_bigquery, read_bigquery


def main():
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")

    print("=" * 80)
    print("Polars BigQuery I/O Plugin (Arrow C Stream + Storage Read API)")
    print("=" * 80)

    # 1. Using Polars' CredentialProviderGCP explicitly
    print("\n--- Example 1: Lazy query with CredentialProviderGCP ---")
    gcp_creds = pl.CredentialProviderGCP(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )

    query = """
        SELECT 
            x AS id,
            CONCAT('item_', CAST(x AS STRING)) AS name,
            CAST(x AS FLOAT64) * 1.5 AS price,
            (MOD(x, 2) = 0) AS is_active
        FROM UNNEST(GENERATE_ARRAY(1, 1000)) AS x
    """

    # Lazy scan: returns a Polars LazyFrame without fetching all rows immediately
    lf = scan_bigquery(
        query=query,
        project_id=project_id,
        credential_provider=gcp_creds,
    )

    # Build an optimized lazy query plan in Polars
    result_df = (
        lf.filter(pl.col("is_active") == True)
        .filter(pl.col("price") > 50.0)
        .select(["id", "name", "price"])
        .limit(10)
        .collect()
    )

    print("Query Result (filtered & projected):")
    print(result_df)

    # 2. Eager execution using default Application Default Credentials (ADC)
    print("\n--- Example 2: Eager read with ADC ---")
    df = read_bigquery(
        query="SELECT 'Hello from accelerated BigQuery Storage Read API' AS greeting, 42 AS answer",
        project_id=project_id,
    )
    print("Eager DataFrame:")
    print(df)


if __name__ == "__main__":
    main()
