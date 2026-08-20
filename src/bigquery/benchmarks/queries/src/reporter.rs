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

use crate::args::Args;
use crate::metrics::{self, LatencySummary};
use crate::sample::{Sample, SampleStatus};
use crate::scenarios::Scenario;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Write;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::Receiver;

/// Structured benchmark summary report.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BenchmarkReport {
    pub scenario: String,
    pub task_count: usize,
    pub total_samples: usize,
    pub success_count: usize,
    pub error_count: usize,
    pub retries_detected_count: usize,
    pub total_rows_read: usize,
    pub total_bytes_processed: i64,
    pub total_duration: Option<LatencySummary>,
    pub send_duration: Option<LatencySummary>,
    pub poll_duration: Option<LatencySummary>,
    pub read_duration: Option<LatencySummary>,
}

impl BenchmarkReport {
    /// Prints the benchmark summary report to stdout.
    pub fn print_stdout(&self) {
        println!("\n=======================================================");
        println!("               BigQuery Benchmark Report               ");
        println!("=======================================================");
        println!("Scenario:                 {}", self.scenario);
        println!("Task Count:               {}", self.task_count);
        println!("Total Queries Executed:   {}", self.total_samples);
        println!("Successful Queries:       {}", self.success_count);
        println!("Errors:                   {}", self.error_count);
        println!("Job Retries Detected:     {}", self.retries_detected_count);
        println!("Total Rows Read:          {}", self.total_rows_read);
        println!("Total Bytes Processed:    {}", self.total_bytes_processed);

        if let Some(total) = &self.total_duration {
            println!("\n--- End-to-End Query Latency ---");
            println!("  Min:   {:?}", total.min);
            println!("  Mean:  {:?}", total.mean);
            println!("  P50:   {:?}", total.p50);
            println!("  P90:   {:?}", total.p90);
            println!("  P99:   {:?}", total.p99);
            println!("  Max:   {:?}", total.max);
        }

        if let Some(send) = &self.send_duration {
            println!("\n--- Query::send() Latency ---");
            println!(
                "  P50:   {:?} | P90: {:?} | P99: {:?}",
                send.p50, send.p90, send.p99
            );
        }

        if let Some(poll) = &self.poll_duration {
            println!("\n--- Query::until_done() Polling Latency ---");
            println!(
                "  P50:   {:?} | P90: {:?} | P99: {:?}",
                poll.p50, poll.p90, poll.p99
            );
        }

        if let Some(read) = &self.read_duration {
            println!("\n--- CompleteQuery::read() Streaming Latency ---");
            println!(
                "  P50:   {:?} | P90: {:?} | P99: {:?}",
                read.p50, read.p90, read.p99
            );
        }

        println!("=======================================================\n");
    }
}

/// Receives sample results, logs real-time output, and generates the final report.
pub async fn collect_and_report(
    mut rx: Receiver<Sample>,
    scenario: &Scenario,
    args: &Args,
) -> anyhow::Result<BenchmarkReport> {
    let mut total_samples = 0_usize;
    let mut success_total_durations = Vec::new();
    let mut success_send_durations = Vec::new();
    let mut success_poll_durations = Vec::new();
    let mut success_read_durations = Vec::new();

    let mut success_count = 0_usize;
    let mut error_count = 0_usize;
    let mut retries_detected_count = 0_usize;
    let mut total_rows_read = 0_usize;
    let mut total_bytes_processed = 0_i64;

    let mut realtime_files = if let Some(output_dir) = &args.output_dir {
        std::fs::create_dir_all(output_dir)?;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let csv_path = output_dir.join(format!("samples-{}-{}.csv", scenario.name, timestamp));
        let json_path = output_dir.join(format!("summary-{}-{}.json", scenario.name, timestamp));

        let mut csv_file = File::create(&csv_path)?;
        writeln!(csv_file, "{}", Sample::HEADER)?;
        csv_file.flush()?;

        println!("Writing real-time samples to: {}", csv_path.display());
        println!("Writing real-time summary to: {}", json_path.display());

        Some((csv_file, csv_path, json_path))
    } else {
        None
    };

    while let Some(sample) = rx.recv().await {
        total_samples += 1;
        if sample.status == SampleStatus::Ok {
            success_count += 1;
            success_total_durations.push(sample.total_duration());
            success_send_durations.push(Duration::from_micros(sample.send_duration_micros as u64));
            success_poll_durations.push(Duration::from_micros(sample.poll_duration_micros as u64));
            success_read_durations.push(Duration::from_micros(sample.read_duration_micros as u64));
            total_rows_read += sample.rows_count;
            total_bytes_processed += sample.bytes_processed;
        } else {
            error_count += 1;
        }

        if sample.retry_detected {
            retries_detected_count += 1;
        }

        if let Some((csv_file, _, json_path)) = &mut realtime_files {
            let _ = writeln!(csv_file, "{}", sample.to_csv_row());
            let _ = csv_file.flush();

            let stats = ReportStats {
                scenario_name: &scenario.name,
                task_count: args.task_count,
                total_samples,
                success_count,
                error_count,
                retries_detected_count,
                total_rows_read,
                total_bytes_processed,
                success_total_durations: &success_total_durations,
                success_send_durations: &success_send_durations,
                success_poll_durations: &success_poll_durations,
                success_read_durations: &success_read_durations,
            };
            let current_report = build_report(&stats);
            if let Ok(json_file) = File::create(json_path) {
                let _ = serde_json::to_writer_pretty(json_file, &current_report);
            }
        }
    }

    let stats = ReportStats {
        scenario_name: &scenario.name,
        task_count: args.task_count,
        total_samples,
        success_count,
        error_count,
        retries_detected_count,
        total_rows_read,
        total_bytes_processed,
        success_total_durations: &success_total_durations,
        success_send_durations: &success_send_durations,
        success_poll_durations: &success_poll_durations,
        success_read_durations: &success_read_durations,
    };
    let report = build_report(&stats);

    report.print_stdout();

    if let Some((_, csv_path, json_path)) = realtime_files {
        println!("Final samples saved to: {}", csv_path.display());
        println!("Final summary saved to: {}", json_path.display());
    }

    Ok(report)
}

struct ReportStats<'a> {
    scenario_name: &'a str,
    task_count: usize,
    total_samples: usize,
    success_count: usize,
    error_count: usize,
    retries_detected_count: usize,
    total_rows_read: usize,
    total_bytes_processed: i64,
    success_total_durations: &'a [Duration],
    success_send_durations: &'a [Duration],
    success_poll_durations: &'a [Duration],
    success_read_durations: &'a [Duration],
}

fn build_report(stats: &ReportStats) -> BenchmarkReport {
    BenchmarkReport {
        scenario: stats.scenario_name.to_string(),
        task_count: stats.task_count,
        total_samples: stats.total_samples,
        success_count: stats.success_count,
        error_count: stats.error_count,
        retries_detected_count: stats.retries_detected_count,
        total_rows_read: stats.total_rows_read,
        total_bytes_processed: stats.total_bytes_processed,
        total_duration: metrics::compute_metrics(stats.success_total_durations),
        send_duration: metrics::compute_metrics(stats.success_send_durations),
        poll_duration: metrics::compute_metrics(stats.success_poll_durations),
        read_duration: metrics::compute_metrics(stats.success_read_durations),
    }
}
