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

use clap::{Parser, ValueEnum};
use google_cloud_bigquery::client::BigQuery;
use google_cloud_bigquery::query::FromRow;
use stats_alloc::{Region, StatsAlloc};
use std::alloc::System;
use std::time::{Duration, Instant};

#[global_allocator]
static GLOBAL: StatsAlloc<System> = StatsAlloc::system();

fn format_bytes(bytes: usize) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.2} KB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}

// ============================================================================
// CLI Arguments and Scenarios
// ============================================================================

#[derive(Parser, Debug)]
#[command(
    name = "bigquery-benchmark-arrow-jobs-query",
    about = "BigQuery Benchmark: Storage Read API vs Standard REST JSON Pagination (Speed & Memory Allocations)"
)]
struct Args {
    /// GCP Project ID (reads from GOOGLE_CLOUD_PROJECT if omitted).
    #[arg(long, env = "GOOGLE_CLOUD_PROJECT")]
    project_id: Option<String>,

    /// Scenario to execute.
    #[arg(long, value_enum, default_value = "synthetic-100k")]
    scenario: Scenario,

    /// Custom query to run when scenario is `custom`.
    #[arg(long)]
    query: Option<String>,

    /// Number of benchmark measurement iterations.
    #[arg(long, default_value_t = 5)]
    iterations: usize,

    /// Number of warmup iterations before measuring.
    #[arg(long, default_value_t = 1)]
    warmup: usize,

    /// Whether to enable BigQuery server-side query cache (default: false).
    #[arg(long, default_value_t = false)]
    use_query_cache: bool,

    /// Whether to deserialize rows into typed structs using FromRow.
    #[arg(long, default_value_t = true)]
    typed: bool,

    /// Explicitly enable or disable Storage Read API acceleration (if omitted and not --compare, defaults to true).
    #[arg(long)]
    storage_read: Option<bool>,

    /// Run both modes (Standard REST JSON vs Storage Read API) and print comparison summary.
    #[arg(long, default_value_t = false)]
    compare: bool,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum Scenario {
    #[value(name = "synthetic-1")]
    Synthetic1,
    #[value(name = "synthetic-100")]
    Synthetic100,
    #[value(name = "synthetic-1k")]
    Synthetic1k,
    #[value(name = "synthetic-10k")]
    Synthetic10k,
    #[value(name = "synthetic-50k")]
    Synthetic50k,
    #[value(name = "synthetic-100k")]
    Synthetic100k,
    #[value(name = "synthetic-500k")]
    Synthetic500k,
    #[value(name = "wikipedia-10k")]
    Wikipedia10k,
    #[value(name = "wikipedia-100k")]
    Wikipedia100k,
    #[value(name = "wikipedia-500k")]
    Wikipedia500k,
    #[value(name = "custom")]
    Custom,
}

impl Scenario {
    fn query(&self, custom_query: Option<&str>) -> String {
        match self {
            Scenario::Synthetic1 => Self::synthetic_query(1),
            Scenario::Synthetic100 => Self::synthetic_query(100),
            Scenario::Synthetic1k => Self::synthetic_query(1_000),
            Scenario::Synthetic10k => Self::synthetic_query(10_000),
            Scenario::Synthetic50k => Self::synthetic_query(50_000),
            Scenario::Synthetic100k => Self::synthetic_query(100_000),
            Scenario::Synthetic500k => Self::synthetic_query(500_000),
            Scenario::Wikipedia10k => Self::wikipedia_query(10_000),
            Scenario::Wikipedia100k => Self::wikipedia_query(100_000),
            Scenario::Wikipedia500k => Self::wikipedia_query(500_000),
            Scenario::Custom => custom_query
                .expect("custom query must be provided when scenario is 'custom'")
                .to_string(),
        }
    }

    fn synthetic_query(row_count: usize) -> String {
        format!(
            "SELECT \
                x AS id, \
                CONCAT('row_item_name_', CAST(x AS STRING)) AS name, \
                CAST(x AS FLOAT64) * 1.25 AS score, \
                (MOD(x, 2) = 0) AS is_even, \
                CURRENT_TIMESTAMP() AS created_at \
             FROM UNNEST(GENERATE_ARRAY(1, {row_count})) AS x"
        )
    }

    fn wikipedia_query(limit: usize) -> String {
        format!(
            "SELECT \
                title, \
                id, \
                language, \
                wp_namespace, \
                is_redirect, \
                revision_id, \
                timestamp, \
                contributor_ip, \
                contributor_id, \
                contributor_username, \
                comment, \
                num_characters \
             FROM `bigquery-public-data.samples.wikipedia` \
             LIMIT {limit}"
        )
    }

    fn is_wikipedia(&self) -> bool {
        matches!(
            self,
            Scenario::Wikipedia10k | Scenario::Wikipedia100k | Scenario::Wikipedia500k
        )
    }
}

#[derive(FromRow, Debug, PartialEq)]
#[allow(dead_code)]
struct SyntheticRow {
    id: i64,
    name: String,
    score: f64,
    is_even: bool,
    created_at: wkt::Timestamp,
}

#[derive(FromRow, Debug, PartialEq)]
#[allow(dead_code)]
struct WikipediaRow {
    title: Option<String>,
    id: Option<i64>,
    language: Option<String>,
    wp_namespace: Option<i64>,
    is_redirect: Option<bool>,
    revision_id: Option<i64>,
    timestamp: Option<i64>,
    contributor_ip: Option<String>,
    contributor_id: Option<i64>,
    contributor_username: Option<String>,
    comment: Option<String>,
    num_characters: Option<i64>,
}

#[derive(Debug, Clone)]
struct IterationResult {
    query_duration: Duration,
    read_duration: Duration,
    total_duration: Duration,
    rows_count: usize,
    bytes_allocated: usize,
    allocations_count: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let project_id = args.project_id.clone().unwrap_or_default();
    let sql_query = args.scenario.query(args.query.as_deref());

    println!("==================================================================================");
    println!("BigQuery Storage Read API Benchmark");
    println!("==================================================================================");
    println!("Scenario:         {:?}", args.scenario);
    println!(
        "Project ID:       {}",
        if project_id.is_empty() {
            "(ADC default)"
        } else {
            &project_id
        }
    );
    println!("Typed:            {}", args.typed);
    println!("Use Query Cache:  {}", args.use_query_cache);
    println!(
        "Iterations:       {} (warmup: {})",
        args.iterations, args.warmup
    );
    println!(
        "Query:\n{}",
        sql_query
            .lines()
            .map(|l| format!("  {l}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
    println!(
        "==================================================================================\n"
    );

    if args.compare {
        println!(">>> Running Mode 1: Standard REST JSON Pagination (storage_read = false)");
        let client_json = BigQuery::builder().with_storage_read(false).build().await?;
        let json_results =
            run_benchmark_suite(&client_json, &project_id, &sql_query, &args, "REST JSON").await?;

        println!(
            "\n>>> Running Mode 2: BigQuery Storage Read API Acceleration (storage_read = true)"
        );
        let client_storage = BigQuery::builder().with_storage_read(true).build().await?;
        let storage_results = run_benchmark_suite(
            &client_storage,
            &project_id,
            &sql_query,
            &args,
            "Storage Read API",
        )
        .await?;

        print_comparison(&json_results, &storage_results);
    } else {
        let storage_read = args.storage_read.unwrap_or(true);
        let mode_label = if storage_read {
            "Storage Read API (Arrow Streaming)"
        } else {
            "Standard REST JSON Pagination"
        };
        println!(">>> Mode: {mode_label} (storage_read = {storage_read})");

        let client = BigQuery::builder()
            .with_storage_read(storage_read)
            .build()
            .await?;
        let results =
            run_benchmark_suite(&client, &project_id, &sql_query, &args, mode_label).await?;
        print_summary(&results, mode_label);
    }

    Ok(())
}

async fn run_benchmark_suite(
    client: &BigQuery,
    project_id: &str,
    sql_query: &str,
    args: &Args,
    label: &str,
) -> anyhow::Result<Vec<IterationResult>> {
    // Warmup runs
    if args.warmup > 0 {
        print!("Warming up ({label}, {} run(s))... ", args.warmup);
        for _ in 0..args.warmup {
            let result = run_single_query(
                client,
                project_id,
                sql_query,
                args.scenario,
                args.use_query_cache,
                args.typed,
            )
            .await?;
            println!(
                "done ({} rows, query: {:.2?}, read: {:.2?}, total: {:.2?}, allocated: {})",
                result.rows_count,
                result.query_duration,
                result.read_duration,
                result.total_duration,
                format_bytes(result.bytes_allocated)
            );
        }
        println!();
    }

    // Benchmark measured runs
    println!("Running {} measurement(s) for {label}...", args.iterations);
    println!(
        "------------------------------------------------------------------------------------------------------------------"
    );
    println!(
        "{:<6} | {:<12} | {:<12} | {:<12} | {:<10} | {:<14} | {:<11} | {:<12}",
        "Run",
        "Query Time",
        "Read/Iter",
        "Total Time",
        "Rows",
        "Throughput",
        "Allocated",
        "Allocations"
    );
    println!(
        "------------------------------------------------------------------------------------------------------------------"
    );

    let mut results = Vec::with_capacity(args.iterations);
    for i in 1..=args.iterations {
        let result = run_single_query(
            client,
            project_id,
            sql_query,
            args.scenario,
            args.use_query_cache,
            args.typed,
        )
        .await?;
        let rps = if result.read_duration.as_secs_f64() > 0.0 {
            result.rows_count as f64 / result.read_duration.as_secs_f64()
        } else {
            0.0
        };

        println!(
            "{:<6} | {:<12.2?} | {:<12.2?} | {:<12.2?} | {:<10} | {:>10.0} rows/s | {:<11} | {:>12}",
            format!("#{i}"),
            result.query_duration,
            result.read_duration,
            result.total_duration,
            result.rows_count,
            rps,
            format_bytes(result.bytes_allocated),
            result.allocations_count
        );
        results.push(result);
    }
    println!(
        "------------------------------------------------------------------------------------------------------------------"
    );

    Ok(results)
}

async fn run_single_query(
    client: &BigQuery,
    project_id: &str,
    query_str: &str,
    scenario: Scenario,
    use_query_cache: bool,
    typed: bool,
) -> anyhow::Result<IterationResult> {
    let region = Region::new(&GLOBAL);
    let start_total = Instant::now();

    // 1. Submit query and wait until complete
    let start_query = Instant::now();
    let mut query_builder = client.query(query_str);
    if !project_id.is_empty() {
        query_builder = query_builder.with_project_id(project_id);
    }
    let complete_query = query_builder
        .set_use_query_cache(use_query_cache)
        .until_done()
        .await?;
    let query_duration = start_query.elapsed();

    // 2. Read and deserialize rows
    let start_read = Instant::now();
    let mut iter = complete_query.read();
    let mut rows_count = 0;

    if typed {
        if scenario.is_wikipedia() {
            while let Some(row_res) = iter.next().await {
                let row = row_res?;
                let _typed_row: WikipediaRow = row.try_into()?;
                rows_count += 1;
            }
        } else {
            while let Some(row_res) = iter.next().await {
                let row = row_res?;
                let _typed_row: SyntheticRow = row.try_into()?;
                rows_count += 1;
            }
        }
    } else {
        while let Some(row_res) = iter.next().await {
            let _row = row_res?;
            rows_count += 1;
        }
    }
    let read_duration = start_read.elapsed();
    let total_duration = start_total.elapsed();

    let stats = region.change();
    let bytes_allocated = stats.bytes_allocated;
    let allocations_count = stats.allocations;

    Ok(IterationResult {
        query_duration,
        read_duration,
        total_duration,
        rows_count,
        bytes_allocated,
        allocations_count,
    })
}

fn print_summary(results: &[IterationResult], label: &str) {
    if results.is_empty() {
        return;
    }

    let n = results.len() as f64;
    let query_times: Vec<f64> = results
        .iter()
        .map(|r| r.query_duration.as_secs_f64())
        .collect();
    let read_times: Vec<f64> = results
        .iter()
        .map(|r| r.read_duration.as_secs_f64())
        .collect();
    let total_times: Vec<f64> = results
        .iter()
        .map(|r| r.total_duration.as_secs_f64())
        .collect();
    let throughputs: Vec<f64> = results
        .iter()
        .map(|r| {
            if r.read_duration.as_secs_f64() > 0.0 {
                r.rows_count as f64 / r.read_duration.as_secs_f64()
            } else {
                0.0
            }
        })
        .collect();
    let bytes_allocs: Vec<f64> = results.iter().map(|r| r.bytes_allocated as f64).collect();
    let alloc_counts: Vec<f64> = results.iter().map(|r| r.allocations_count as f64).collect();

    let avg = |v: &[f64]| v.iter().sum::<f64>() / n;
    let min = |v: &[f64]| v.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = |v: &[f64]| v.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let std_dev = |v: &[f64], mean: f64| {
        let variance = v.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
        variance.sqrt()
    };

    let q_avg = avg(&query_times);
    let r_avg = avg(&read_times);
    let t_avg = avg(&total_times);
    let tp_avg = avg(&throughputs);
    let bytes_avg = avg(&bytes_allocs);
    let count_avg = avg(&alloc_counts);

    let rows_avg = results[0].rows_count as f64;
    let bytes_per_row = if rows_avg > 0.0 {
        bytes_avg / rows_avg
    } else {
        0.0
    };

    println!(
        "\nSummary Statistics for {label} (over {} runs):",
        results.len()
    );
    println!(
        "  Query Execution Time:   avg: {:.2?} (min: {:.2?}, max: {:.2?}, stddev: {:.2?})",
        Duration::from_secs_f64(q_avg),
        Duration::from_secs_f64(min(&query_times)),
        Duration::from_secs_f64(max(&query_times)),
        Duration::from_secs_f64(std_dev(&query_times, q_avg))
    );
    println!(
        "  Row Reading & Parsing:  avg: {:.2?} (min: {:.2?}, max: {:.2?}, stddev: {:.2?})",
        Duration::from_secs_f64(r_avg),
        Duration::from_secs_f64(min(&read_times)),
        Duration::from_secs_f64(max(&read_times)),
        Duration::from_secs_f64(std_dev(&read_times, r_avg))
    );
    println!(
        "  Total End-to-End Time:  avg: {:.2?} (min: {:.2?}, max: {:.2?}, stddev: {:.2?})",
        Duration::from_secs_f64(t_avg),
        Duration::from_secs_f64(min(&total_times)),
        Duration::from_secs_f64(max(&total_times)),
        Duration::from_secs_f64(std_dev(&total_times, t_avg))
    );
    println!(
        "  Row Throughput:         avg: {:.0} rows/s (min: {:.0}, max: {:.0})",
        tp_avg,
        min(&throughputs),
        max(&throughputs)
    );
    println!(
        "  Total Heap Allocated:   avg: {} ({:.1} bytes/row)",
        format_bytes(bytes_avg as usize),
        bytes_per_row
    );
    println!(
        "  Total Allocations:      avg: {:.0} allocs ({:.1} allocs/row)",
        count_avg,
        if rows_avg > 0.0 {
            count_avg / rows_avg
        } else {
            0.0
        }
    );
}

fn print_comparison(json_results: &[IterationResult], storage_results: &[IterationResult]) {
    if json_results.is_empty() || storage_results.is_empty() {
        return;
    }

    let n_j = json_results.len() as f64;
    let n_s = storage_results.len() as f64;

    let avg_read_json = json_results
        .iter()
        .map(|r| r.read_duration.as_secs_f64())
        .sum::<f64>()
        / n_j;
    let avg_read_storage = storage_results
        .iter()
        .map(|r| r.read_duration.as_secs_f64())
        .sum::<f64>()
        / n_s;

    let avg_total_json = json_results
        .iter()
        .map(|r| r.total_duration.as_secs_f64())
        .sum::<f64>()
        / n_j;
    let avg_total_storage = storage_results
        .iter()
        .map(|r| r.total_duration.as_secs_f64())
        .sum::<f64>()
        / n_s;

    let avg_tp_json = json_results
        .iter()
        .map(|r| r.rows_count as f64 / r.read_duration.as_secs_f64())
        .sum::<f64>()
        / n_j;
    let avg_tp_storage = storage_results
        .iter()
        .map(|r| r.rows_count as f64 / r.read_duration.as_secs_f64())
        .sum::<f64>()
        / n_s;

    let avg_mem_json = json_results
        .iter()
        .map(|r| r.bytes_allocated as f64)
        .sum::<f64>()
        / n_j;
    let avg_mem_storage = storage_results
        .iter()
        .map(|r| r.bytes_allocated as f64)
        .sum::<f64>()
        / n_s;

    let avg_alloc_json = json_results
        .iter()
        .map(|r| r.allocations_count as f64)
        .sum::<f64>()
        / n_j;
    let avg_alloc_storage = storage_results
        .iter()
        .map(|r| r.allocations_count as f64)
        .sum::<f64>()
        / n_s;

    let speedup_read = avg_read_json / avg_read_storage;
    let speedup_total = avg_total_json / avg_total_storage;
    let mem_reduction = (avg_mem_json - avg_mem_storage) / avg_mem_json * 100.0;
    let alloc_reduction = (avg_alloc_json - avg_alloc_storage) / avg_alloc_json * 100.0;

    println!(
        "\n=================================================================================="
    );
    println!("COMPARISON: Standard REST JSON vs Storage Read API");
    println!("==================================================================================");
    println!(
        "{:<26} | {:<22} | {:<22} | {:<12}",
        "Metric", "REST JSON", "Storage Read API", "Improvement"
    );
    println!("----------------------------------------------------------------------------------");
    println!(
        "{:<26} | {:<22.2?} | {:<22.2?} | {:>.2}x faster",
        "Read/Iter Time (avg)",
        Duration::from_secs_f64(avg_read_json),
        Duration::from_secs_f64(avg_read_storage),
        speedup_read
    );
    println!(
        "{:<26} | {:<22.2?} | {:<22.2?} | {:>.2}x faster",
        "Total Time (avg)",
        Duration::from_secs_f64(avg_total_json),
        Duration::from_secs_f64(avg_total_storage),
        speedup_total
    );
    println!(
        "{:<26} | {:>15.0} rows/s | {:>15.0} rows/s | {:>.2}x throughput",
        "Throughput (avg)",
        avg_tp_json,
        avg_tp_storage,
        avg_tp_storage / avg_tp_json
    );
    println!(
        "{:<26} | {:<22} | {:<22} | {:>.1}% less",
        "Heap Allocated (avg)",
        format_bytes(avg_mem_json as usize),
        format_bytes(avg_mem_storage as usize),
        mem_reduction
    );
    println!(
        "{:<26} | {:>15.0} allocs | {:>15.0} allocs | {:>.1}% less",
        "Allocations (avg)", avg_alloc_json, avg_alloc_storage, alloc_reduction
    );
    println!(
        "==================================================================================\n"
    );
}
