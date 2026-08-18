# BigQuery SDK Benchmark & Endurance Test Suite

Benchmarks the Rust BigQuery client library (`google-cloud-bigquery`), measuring latency distributions (P50/P90/P99), query throughput, and endurance under long-running workloads with OpenTelemetry tracking and under-the-hood job retry detection.

## Features

- **Benchmark & Endurance Modes**: Run fixed iterations or continuous duration-based tests (e.g. 10m, 2h, 24h).
- **Under-the-Hood Retry Detection**: Detects BigQuery job retries by checking if the `job_id` changed between `Query::send()` and `Query::until_done()`.
- **OpenTelemetry & Cloud Observability**: Automatically exports distributed traces to **Google Cloud Trace** and metric instruments to **Google Cloud Monitoring** when `--project-id` is provided.
- **Configurable Scenarios**:
  - `synthetic-100k` (default): Zero-dependency query generating 100,000 structured rows in-flight with `UNNEST(GENERATE_ARRAY(1, 100000))`.
  - `synthetic-10k`: Zero-dependency query generating 10,000 rows.
  - `usa-names-scan`: Scans and retrieves 50,000 rows from `bigquery-public-data.usa_names.usa_1910_2013`.
  - `usa-names-agg`: Aggregates 5.5M rows grouped by state and gender.
  - `wikipedia-agg`: Aggregates top 1000 page views from Wikipedia public dataset.
  - `custom`: Executes user-provided queries via `--sql` or `--sql-file`.

---

## Pre-requisites

1. **Authentication**:
   Ensure Application Default Credentials (ADC) are configured:
   ```shell
   gcloud auth application-default login
   ```

2. **Project ID**:
   Set the project ID environment variable:
   ```shell
   export GOOGLE_CLOUD_PROJECT="$(gcloud config get project)"
   ```

---

## Running Benchmarks

### 1. Zero-Setup Synthetic Benchmark (Default)

Runs 10 iterations per task with 4 concurrent tasks, generating and streaming 100,000 rows per query:

```shell
cargo run --release -p bigquery-benchmark-queries -- \
    --project-id ${GOOGLE_CLOUD_PROJECT} \
    --scenario synthetic-100k \
    --task-count 4 \
    --iterations 10 \
    --output-dir ./results
```

### 2. Public Dataset Query Benchmark

Benchmark streaming 50,000 rows from the USA names public dataset:

```shell
cargo run --release -p bigquery-benchmark-queries -- \
    --project-id ${GOOGLE_CLOUD_PROJECT} \
    --scenario usa-names-scan \
    --task-count 2 \
    --iterations 20 \
    --output-dir ./results
```

### 3. Custom SQL Query Benchmark

```shell
cargo run --release -p bigquery-benchmark-queries -- \
    --project-id ${GOOGLE_CLOUD_PROJECT} \
    --scenario custom \
    --sql "SELECT word, SUM(word_count) as total FROM \`bigquery-public-data.samples.shakespeare\` GROUP BY word ORDER BY total DESC LIMIT 100" \
    --task-count 4 \
    --iterations 25
```

---

## Running Endurance Tests

To test connection stability, memory leaks, and token refreshing over a prolonged period (e.g. 1 hour):

```shell
cargo run --release -p bigquery-benchmark-queries -- \
    --project-id ${GOOGLE_CLOUD_PROJECT} \
    --scenario synthetic-100k \
    --task-count 4 \
    --duration 1h \
    --output-dir ./results
```

---

## OpenTelemetry & Cloud Observability

When `--project-id` is specified, the benchmark automatically connects to `telemetry.googleapis.com` and records:

### Cloud Monitoring Metrics
- `bigquery.queries.total`: Total count of executed queries.
- `bigquery.queries.success`: Successful query executions.
- `bigquery.queries.error`: Query failures.
- `bigquery.queries.retries_detected`: Queries where an under-the-hood job retry was detected.
- `bigquery.queries.rows_read`: Cumulative rows streamed.
- `bigquery.queries.bytes_processed`: Total bytes processed.
- `bigquery.queries.duration_seconds`: Histogram of total end-to-end query latency.
- `bigquery.queries.send_duration_seconds`: Histogram of `Query::send()` latency.
- `bigquery.queries.poll_duration_seconds`: Histogram of `Query::until_done()` polling latency.
- `bigquery.queries.read_duration_seconds`: Histogram of `CompleteQuery::read()` row streaming latency.

### Cloud Trace Spans
- Root span: `bigquery.query_benchmark.iteration`
- Child spans:
  - `bigquery.send`
  - `bigquery.until_done`
  - `bigquery.read_rows`

---

## Uploading Results to BigQuery

If `--output-dir` is specified, the suite saves per-iteration raw samples to a CSV file (e.g. `results/samples-synthetic-100k-*.csv`). You can upload the samples to BigQuery for SQL analysis:

```shell
bq load --source_format=CSV --skip_leading_rows=1 \
    ${GOOGLE_CLOUD_PROJECT}:benchmark_dataset.bigquery_samples \
    ./results/samples-*.csv \
    Task:int64,Iteration:int64,StartOffsetMicros:int64,SendDurationMicros:int64,PollDurationMicros:int64,ReadDurationMicros:int64,TotalDurationMicros:int64,RowsCount:int64,BytesProcessed:int64,CacheHit:bool,InitialJobId:string,FinalJobId:string,RetryDetected:bool,Status:string,ErrorMessage:string
```
