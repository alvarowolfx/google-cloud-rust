# Polars BigQuery I/O Plugin (Arrow C Stream & Storage Read API)

A high-performance Polars I/O plugin and LazyFrame data source for Google Cloud BigQuery, built on top of `google-cloud-rust`.

## Key Features

1. **Zero-Copy Arrow C Stream FFI Interface**:
   Leverages BigQuery query execution's `to_arrow_c_stream()` (`FFI_ArrowArrayStream`) which conforms strictly to the [Apache Arrow C Stream Interface specification](https://arrow.apache.org/docs/format/CStreamInterface.html). Data is exchanged across the Rust/Python FFI boundary using the standard Arrow PyCapsule protocol (`__arrow_c_stream__`) without serialization or copying.

2. **BigQuery Storage Read API Acceleration**:
   Queries stream query result partitions concurrently using gRPC and Apache Arrow streaming rather than JSON REST pagination.

3. **Pluggable GCP Authentication (Zero SDK Changes)**:
   Seamlessly bridges Polars' built-in `pl.CredentialProviderGCP()` (and custom Python token callables) into `google_cloud_auth::credentials::CredentialsProvider` without modifying any Rust SDK crates. Also supports standard Google Application Default Credentials (ADC) out of the box.

4. **Polars Lazy Engine Integration**:
   Provides `scan_bigquery(...)` returning `pl.LazyFrame` with predicate and projection pushdown capabilities.

---

## Installation & Building

```bash
# In the demos/polars-bigquery directory
pip install maturin
maturin develop
```

---

## Usage Examples

### 1. Lazy Scan with Polars `CredentialProviderGCP`

```python
import polars as pl
from polars_bigquery import scan_bigquery

# Use Polars' built-in GCP credential provider utility
gcp_creds = pl.CredentialProviderGCP(
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
)

# Create a LazyFrame
lf = scan_bigquery(
    query="SELECT id, name, score FROM `my_project.my_dataset.my_table`",
    credential_provider=gcp_creds,
)

# Apply optimizations and evaluate lazily
df = (
    lf.filter(pl.col("score") > 75.0)
    .select(["id", "name", "score"])
    .limit(100)
    .collect()
)

print(df)
```

### 2. Eager Query with Application Default Credentials (ADC)

```python
from polars_bigquery import read_bigquery

# Reads directly into a Polars DataFrame using environment ADC
df = read_bigquery("SELECT 'hello' AS title, 100 AS count")
print(df)
```

---

## How It Works Under the Hood

```
┌────────────────────────────────────────────────────────┐
│                     Polars Python                      │
│   scan_bigquery() / pl.scan_arrow_c_stream()           │
└───────────────────────────▲────────────────────────────┘
                            │ Arrow PyCapsule FFI
                            │ ("__arrow_c_stream__")
┌───────────────────────────┴────────────────────────────┐
│              _polars_bigquery (PyO3)                   │
│   BigQueryArrowStream (ArrowArrayStream FFI)           │
│   PyPolarsCredentialProvider                           │
└───────────────────────────▲────────────────────────────┘
                            │
┌───────────────────────────┴────────────────────────────┐
│                google-cloud-rust                       │
│   google-cloud-bigquery (to_arrow_c_stream)            │
│   google-cloud-auth     (CredentialsProvider)          │
└───────────────────────────▲────────────────────────────┘
                            │ gRPC Storage Read API
┌───────────────────────────┴────────────────────────────┐
│                Google Cloud BigQuery                   │
└────────────────────────────────────────────────────────┘
```
