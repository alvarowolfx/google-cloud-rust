// Copyright 2025 Google LLC
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

use anyhow::Result;
use futures::stream::StreamExt;
use google_cloud_bigquery_v2::client::{DatasetService, JobService};
use google_cloud_bigquery_v2::model::query_request::JobCreationMode;
use google_cloud_bigquery_v2::model::{
    Dataset, DatasetReference, Job, JobConfiguration, JobConfigurationQuery, JobReference,
};
use google_cloud_gax::error::rpc::Code;
use google_cloud_gax::paginator::ItemPaginator;
use google_cloud_test_utils::runtime_config::project_id;
use rand::{RngExt, distr::Alphanumeric};

const INSTANCE_LABEL: &str = "rust-sdk-integration-test";

pub async fn dataset_admin() -> Result<()> {
    let project_id = project_id()?;
    let client = DatasetService::builder().with_tracing().build().await?;
    cleanup_stale_datasets(&client, &project_id).await?;

    let dataset_id = random_dataset_id();

    println!("CREATING DATASET WITH ID: {dataset_id}");

    let create = client
        .insert_dataset()
        .set_project_id(&project_id)
        .set_dataset(
            Dataset::new()
                .set_dataset_reference(DatasetReference::new().set_dataset_id(&dataset_id))
                .set_labels([(INSTANCE_LABEL, "true")]),
        )
        .send()
        .await?;
    println!("CREATE DATASET = {create:?}");

    assert!(create.dataset_reference.is_some(), "{create:?}");

    let list = client
        .list_datasets()
        .set_project_id(&project_id)
        .set_filter(format!("labels.{INSTANCE_LABEL}"))
        .by_item()
        .into_stream();
    let items = list.collect::<Vec<_>>().await;
    println!("LIST DATASET = {} entries", items.len());

    assert!(
        items
            .iter()
            .any(|v| v.as_ref().unwrap().id.contains(&dataset_id))
    );

    client
        .delete_dataset()
        .set_project_id(&project_id)
        .set_dataset_id(&dataset_id)
        .set_delete_contents(true)
        .send()
        .await?;
    println!("DELETE DATASET");

    Ok(())
}

async fn cleanup_stale_datasets(client: &DatasetService, project_id: &str) -> Result<()> {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let stale_deadline = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let stale_deadline = stale_deadline - Duration::from_secs(48 * 60 * 60);
    let stale_deadline = stale_deadline.as_millis() as i64;

    let list = client
        .list_datasets()
        .set_project_id(project_id)
        .set_filter(format!("labels.{INSTANCE_LABEL}"))
        .by_item()
        .into_stream();
    let datasets = list.collect::<Vec<_>>().await;

    let pending_all_datasets = datasets
        .iter()
        .filter_map(|v| match v {
            Ok(v) => {
                if let Some(dataset_id) = extract_dataset_id(project_id, &v.id) {
                    return Some(
                        client
                            .get_dataset()
                            .set_project_id(project_id)
                            .set_dataset_id(dataset_id)
                            .send(),
                    );
                }
                None
            }
            Err(_) => None,
        })
        .collect::<Vec<_>>();

    let stale_datasets = futures::future::join_all(pending_all_datasets)
        .await
        .into_iter()
        .filter_map(|r| match r {
            Ok(dataset) => Some(dataset),
            Err(e) if e.status().is_some_and(|s| s.code == Code::NotFound) => None,
            Err(_) => panic!("expected a successful get_dataset()"),
        })
        .filter_map(|dataset| {
            if dataset
                .labels
                .get(INSTANCE_LABEL)
                .is_some_and(|v| v == "true")
                && dataset.creation_time < stale_deadline
            {
                return Some(dataset);
            }
            None
        })
        .collect::<Vec<_>>();

    println!("found {} stale datasets", stale_datasets.len());

    let pending_deletion: Vec<_> = stale_datasets
        .into_iter()
        .filter_map(|ds| {
            if let Some(dataset_id) = extract_dataset_id(project_id, &ds.id) {
                return Some(
                    client
                        .delete_dataset()
                        .set_project_id(project_id)
                        .set_dataset_id(dataset_id)
                        .set_delete_contents(true)
                        .send(),
                );
            }
            None
        })
        .collect();

    futures::future::join_all(pending_deletion).await;

    Ok(())
}

fn random_dataset_id() -> String {
    let rand_suffix = random_id_suffix();
    format!("rust_bq_test_dataset_{rand_suffix}")
}

fn random_job_id() -> String {
    let rand_suffix = random_id_suffix();
    format!("rust_bq_test_job_{rand_suffix}")
}

fn random_id_suffix() -> String {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(8)
        .map(char::from)
        .collect()
}

fn extract_dataset_id(project_id: &str, id: &str) -> Option<String> {
    id.strip_prefix(format!("{project_id}:").as_str())
        .map(|v| v.to_string())
}

pub async fn job_service() -> Result<()> {
    let project_id = project_id()?;
    let client = JobService::builder().with_tracing().build().await?;
    cleanup_stale_jobs(&client, &project_id).await?;

    let job_id = random_job_id();
    println!("CREATING JOB WITH ID: {job_id}");

    let query = "SELECT 1 as one";
    let job = client
        .insert_job()
        .set_project_id(&project_id)
        .set_job(
            Job::new()
                .set_job_reference(JobReference::new().set_job_id(&job_id))
                .set_configuration(
                    JobConfiguration::new()
                        .set_labels([(INSTANCE_LABEL, "true")])
                        .set_query(JobConfigurationQuery::new().set_query(query)),
                ),
        )
        .send()
        .await?;
    println!("CREATE JOB = {job:?}");

    assert!(job.job_reference.is_some(), "{job:?}");

    let list = client
        .list_jobs()
        .set_project_id(&project_id)
        .by_item()
        .into_stream();
    let items = list.collect::<Vec<_>>().await;
    println!("LIST JOBS = {} entries", items.len());

    assert!(
        items
            .iter()
            .any(|v| v.as_ref().unwrap().id.contains(&job_id))
    );

    Ok(())
}

pub async fn query_client() -> Result<()> {
    let project_id = project_id()?;
    let bq = google_cloud_bigquery::client::BigQuery::builder()
        .build()
        .await?;

    println!("STARTING HIGH-LEVEL SMOKE TEST QUERY");
    let query = bq
        .query("SELECT 1 as one")
        .set_job_creation_mode(JobCreationMode::JobCreationOptional)
        .with_project_id(project_id)
        .set_labels(vec![(INSTANCE_LABEL, "true")])
        .run()
        .await?;

    assert!(query.query_id().is_some());

    let complete_query = query.until_done().await?;

    assert_eq!(complete_query.metadata().total_rows, Some(1));

    let mut rows = complete_query.read();
    let mut count = 0;
    while let Some(row) = rows.next().await {
        let _row = row?;
        count += 1;
    }

    println!("READ {count} ROWS SUCCESSFULLY");
    assert_eq!(count, 1);

    Ok(())
}

pub async fn query_client_multi_page() -> Result<()> {
    let project_id = project_id()?;
    let bq = google_cloud_bigquery::client::BigQuery::builder()
        .build()
        .await?;

    println!("STARTING HIGH-LEVEL MULTI-PAGE QUERY");
    let query = bq
        .query("SELECT * FROM UNNEST(GENERATE_ARRAY(1, 10000)) AS val")
        .set_use_legacy_sql(false)
        .set_max_results(1000_u32)
        .with_project_id(project_id)
        .set_labels(vec![(INSTANCE_LABEL, "true")])
        .run()
        .await?;

    let complete_query = query.until_done().await?;

    assert_eq!(complete_query.metadata().total_rows, Some(10000));

    let mut rows = complete_query.read().with_max_results(1000);
    let mut count = 0;
    while let Some(row) = rows.next().await {
        let _row = row?;
        count += 1;
    }

    println!("READ {count} ROWS SUCCESSFULLY ACROSS MULTIPLE PAGES");
    assert_eq!(count, 10000);

    Ok(())
}

pub async fn query_client_job() -> Result<()> {
    let project_id = project_id()?;
    let bq = google_cloud_bigquery::client::BigQuery::builder()
        .build()
        .await?;

    println!("STARTING HIGH-LEVEL QUERY FROM JOB");
    let query = bq
        .query("SELECT 2 as two")
        .set_use_legacy_sql(false)
        .set_priority("INTERACTIVE")
        .with_project_id(&project_id)
        .set_labels(vec![(INSTANCE_LABEL, "true")])
        .run()
        .await?;

    let complete_query = query.until_done().await?;

    assert_eq!(complete_query.metadata().total_rows, Some(1));

    let mut rows = complete_query.read();
    let mut count = 0;
    while let Some(row) = rows.next().await {
        let _row = row?;
        count += 1;
    }

    println!("READ {count} ROWS SUCCESSFULLY");
    assert_eq!(count, 1);

    Ok(())
}

pub async fn query_client_row_parsing() -> Result<()> {
    let project_id = project_id()?;
    let bq = google_cloud_bigquery::client::BigQuery::builder()
        .build()
        .await?;

    println!("STARTING ROW PARSING INTEGRATION TEST");
    let sql = "SELECT \
                 'John Doe' AS name, \
                 30 AS age, \
                 1.85 AS height, \
                 true AS active, \
                 TIMESTAMP '2026-05-28 15:30:00 UTC' AS created_at, \
                 DATE '2026-05-28' AS birth_date, \
                 TIME '15:30:00' AS daily_alarm, \
                 DATETIME '2026-05-28 15:30:00' AS event_time, \
                 CAST(NULL AS STRING) AS nullable_name, \
                 CAST(NULL AS INT64) AS nullable_age, \
                 INTERVAL '1 2:30:45.123456' DAY TO SECOND AS interval_val";

    let query = bq
        .query(sql)
        .set_use_legacy_sql(false)
        .with_project_id(project_id)
        .set_labels(vec![(INSTANCE_LABEL, "true")])
        .run()
        .await?;

    let complete_query = query.until_done().await?;
    let mut rows = complete_query.read();

    if let Some(row) = rows.next().await {
        let row = row?;

        // 1. Verify getting by index
        let name_idx: String = row.get(0);
        let age_idx: i64 = row.get(1);
        let height_idx: f64 = row.get(2);
        let active_idx: bool = row.get(3);

        assert_eq!(name_idx, "John Doe");
        assert_eq!(age_idx, 30);
        assert_eq!(height_idx, 1.85);
        assert!(active_idx);

        // 2. Verify getting by name
        let name: String = row.get("name");
        let age: i64 = row.get("age");
        let height: f64 = row.get("height");
        let active: bool = row.get("active");

        assert_eq!(name, "John Doe");
        assert_eq!(age, 30);
        assert_eq!(height, 1.85);
        assert!(active);

        // 3. Verify date and time conversions
        let created_at: wkt::Timestamp = row.get("created_at");
        assert_eq!(created_at.seconds(), 1779982200); // 2026-05-28 15:30:00 UTC
        assert_eq!(created_at.nanos(), 0);

        let birth_date: google_cloud_type::model::Date = row.get("birth_date");
        assert_eq!(birth_date.year, 2026);
        assert_eq!(birth_date.month, 5);
        assert_eq!(birth_date.day, 28);

        let daily_alarm: google_cloud_type::model::TimeOfDay = row.get("daily_alarm");
        assert_eq!(daily_alarm.hours, 15);
        assert_eq!(daily_alarm.minutes, 30);
        assert_eq!(daily_alarm.seconds, 0);

        let event_time: google_cloud_type::model::DateTime = row.get("event_time");
        assert_eq!(event_time.year, 2026);
        assert_eq!(event_time.month, 5);
        assert_eq!(event_time.day, 28);
        assert_eq!(event_time.hours, 15);
        assert_eq!(event_time.minutes, 30);
        assert_eq!(event_time.seconds, 0);

        // 4. Verify nullable columns (Option<T>)
        let nullable_name: Option<String> = row.get("nullable_name");
        let nullable_age: Option<i64> = row.get("nullable_age");
        assert_eq!(nullable_name, None);
        assert_eq!(nullable_age, None);

        let populated_name: Option<String> = row.get("name");
        assert_eq!(populated_name, Some("John Doe".to_string()));

        let interval_val: google_cloud_bigquery::Interval = row.get("interval_val");
        assert_eq!(interval_val.years, 0);
        assert_eq!(interval_val.months, 0);
        assert_eq!(interval_val.days, 1);
        assert_eq!(interval_val.hours, 2);
        assert_eq!(interval_val.minutes, 30);
        assert_eq!(interval_val.seconds, 45);
        assert_eq!(interval_val.nanos, 123456000);
    } else {
        panic!("expected at least one row");
    }

    println!("ROW PARSING INTEGRATION TEST COMPLETED SUCCESSFULLY");
    Ok(())
}

#[derive(serde::Deserialize, Debug, PartialEq)]
struct UserRecord {
    name: String,
    age: i64,
}

#[derive(serde::Deserialize, Debug, PartialEq)]
struct UserProfile {
    name: String,
    age: i64,
    #[serde(deserialize_with = "google_cloud_bigquery::deserialize")]
    birth_date: google_cloud_type::model::Date,
}

pub async fn query_client_nested_types() -> Result<()> {
    let project_id = project_id()?;
    let bq = google_cloud_bigquery::client::BigQuery::builder()
        .build()
        .await?;

    println!("STARTING NESTED TYPES INTEGRATION TEST");
    let sql = "SELECT \
                 STRUCT('Alice' AS name, 25 AS age) AS user, \
                 ARRAY[1, 2, 3] AS numbers, \
                 ARRAY[STRUCT('Bob' AS name, 28 AS age), STRUCT('Charlie' AS name, 31 AS age)] AS users, \
                 STRUCT('Dave' AS name, 40 AS age, DATE '1986-05-28' AS birth_date) AS profile";

    let query = bq
        .query(sql)
        .set_use_legacy_sql(false)
        .with_project_id(project_id)
        .set_labels(vec![(INSTANCE_LABEL, "true")])
        .run()
        .await?;

    let complete_query = query.until_done().await?;
    let mut rows = complete_query.read();

    if let Some(row) = rows.next().await {
        let row = row?;

        // 1. Verify nested struct parsed into user-defined struct
        let user: UserRecord = serde_json::from_value(row.get::<wkt::Value, _>("user"))?;
        assert_eq!(user.name, "Alice");
        assert_eq!(user.age, 25);

        // 2. Verify repeated basic type (ARRAY)
        let numbers: Vec<i64> = row.get("numbers");
        assert_eq!(numbers, vec![1, 2, 3]);

        // 3. Verify repeated struct parsed into user-defined structs
        let users: Vec<UserRecord> = serde_json::from_value(row.get::<wkt::Value, _>("users"))?;
        assert_eq!(users.len(), 2);
        assert_eq!(users[0].name, "Bob");
        assert_eq!(users[0].age, 28);
        assert_eq!(users[1].name, "Charlie");
        assert_eq!(users[1].age, 31);

        // 4. Verify user-defined struct with BQ-specific date field using the deserialize helper
        let profile: UserProfile = serde_json::from_value(row.get::<wkt::Value, _>("profile"))?;
        assert_eq!(profile.name, "Dave");
        assert_eq!(profile.age, 40);
        assert_eq!(profile.birth_date.year, 1986);
        assert_eq!(profile.birth_date.month, 5);
        assert_eq!(profile.birth_date.day, 28);
    } else {
        panic!("expected at least one row");
    }

    println!("NESTED TYPES INTEGRATION TEST COMPLETED SUCCESSFULLY");
    Ok(())
}

async fn cleanup_stale_jobs(client: &JobService, project_id: &str) -> Result<()> {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let stale_deadline = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let stale_deadline = stale_deadline - Duration::from_secs(48 * 60 * 60);
    let stale_deadline = stale_deadline.as_millis() as u64;

    let list = client
        .list_jobs()
        .set_project_id(project_id)
        .set_max_creation_time(stale_deadline)
        .by_item()
        .into_stream();
    let items = list.collect::<Vec<_>>().await;
    println!("LIST JOBS = {} entries", items.len());

    let pending_all_stale_jobs = items
        .iter()
        .filter_map(|v| match v {
            Ok(v) => {
                if let Some(job_reference) = &v.job_reference {
                    return Some(
                        client
                            .get_job()
                            .set_project_id(project_id)
                            .set_job_id(&job_reference.job_id)
                            .send(),
                    );
                }
                None
            }
            Err(_) => None,
        })
        .collect::<Vec<_>>();

    let pending_deletion = futures::future::join_all(pending_all_stale_jobs)
        .await
        .into_iter()
        .filter_map(|r| match r {
            Ok(r) => {
                let job_reference = r.job_reference?;
                if r.configuration
                    .is_some_and(|c| c.labels.get(INSTANCE_LABEL).is_some_and(|v| v == "true"))
                    && r.status.is_some_and(|s| s.state == "DONE")
                {
                    return Some(
                        client
                            .delete_job()
                            .set_project_id(project_id)
                            .set_job_id(&job_reference.job_id)
                            .send(),
                    );
                }
                None
            }
            Err(_) => None,
        })
        .collect::<Vec<_>>();

    println!("found {} stale test jobs", pending_deletion.len());

    futures::future::join_all(pending_deletion).await;
    Ok(())
}

pub async fn query_client_range_values() -> Result<()> {
    let project_id = project_id()?;
    let bq = google_cloud_bigquery::client::BigQuery::builder()
        .build()
        .await?;

    println!("STARTING RANGE VALUES INTEGRATION TEST");
    let sql = "SELECT \
                 RANGE(DATE '2026-05-28', DATE '2026-05-29') AS date_range, \
                 RANGE(TIMESTAMP '2026-05-28 15:30:00 UTC', NULL) AS timestamp_range";

    let query = bq
        .query(sql)
        .set_use_legacy_sql(false)
        .with_project_id(project_id)
        .run()
        .await?;

    let complete_query = query.until_done().await?;
    let mut rows = complete_query.read();

    if let Some(row) = rows.next().await {
        let row = row?;

        // 1. Verify RANGE<DATE>
        let date_range: google_cloud_bigquery::Range<google_cloud_type::model::Date> =
            row.get("date_range");
        let start_date = date_range.start.unwrap();
        assert_eq!(start_date.year, 2026);
        assert_eq!(start_date.month, 5);
        assert_eq!(start_date.day, 28);

        let end_date = date_range.end.unwrap();
        assert_eq!(end_date.year, 2026);
        assert_eq!(end_date.month, 5);
        assert_eq!(end_date.day, 29);

        // 2. Verify unbounded-end RANGE<TIMESTAMP>
        let timestamp_range: google_cloud_bigquery::Range<wkt::Timestamp> =
            row.get("timestamp_range");
        let start_ts = timestamp_range.start.unwrap();
        assert_eq!(start_ts.seconds(), 1779982200); // 2026-05-28 15:30:00 UTC
        assert_eq!(start_ts.nanos(), 0);

        assert_eq!(timestamp_range.end, None); // Unbounded end
    } else {
        panic!("expected at least one row");
    }

    println!("RANGE VALUES INTEGRATION TEST COMPLETED SUCCESSFULLY");
    Ok(())
}

pub async fn query_client_numeric_limits() -> Result<()> {
    let project_id = project_id()?;
    let bq = google_cloud_bigquery::client::BigQuery::builder()
        .build()
        .await?;

    println!("STARTING NUMERIC/BIGNUMERIC LIMITS INTEGRATION TEST");
    let sql = "SELECT \
                 CAST('99999999999999999999999999999.999999999' AS NUMERIC) AS max_numeric, \
                 CAST('-99999999999999999999999999999.999999999' AS NUMERIC) AS min_numeric, \
                 CAST('578960446186580977117854925043439539266.34992332820282019728792003956564819967' AS BIGNUMERIC) AS max_bignumeric, \
                 CAST('-578960446186580977117854925043439539266.34992332820282019728792003956564819968' AS BIGNUMERIC) AS min_bignumeric";

    let query = bq
        .query(sql)
        .set_use_legacy_sql(false)
        .with_project_id(project_id)
        .run()
        .await?;

    let complete_query = query.until_done().await?;
    let mut rows = complete_query.read();

    if let Some(row) = rows.next().await {
        let row = row?;

        let max_num: google_cloud_type::model::Decimal = row.get("max_numeric");
        let min_num: google_cloud_type::model::Decimal = row.get("min_numeric");
        let max_bignum: google_cloud_type::model::Decimal = row.get("max_bignumeric");
        let min_bignum: google_cloud_type::model::Decimal = row.get("min_bignumeric");

        assert_eq!(max_num.value, "99999999999999999999999999999.999999999");
        assert_eq!(min_num.value, "-99999999999999999999999999999.999999999");
        assert_eq!(
            max_bignum.value,
            "578960446186580977117854925043439539266.34992332820282019728792003956564819967"
        );
        assert_eq!(
            min_bignum.value,
            "-578960446186580977117854925043439539266.34992332820282019728792003956564819968"
        );
    } else {
        panic!("expected at least one row");
    }

    println!("NUMERIC/BIGNUMERIC LIMITS INTEGRATION TEST COMPLETED SUCCESSFULLY");
    Ok(())
}
