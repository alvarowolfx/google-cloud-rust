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

// AUTO-GENERATED CODE. DO NOT EDIT MANUALLY. RUN THE GENERATOR SCRIPT TO UPDATE.

use super::RunQuery;

#[allow(clippy::clone_on_copy)]
impl RunQuery {
    /// Sets the value of [allow_large_results][crate::model::JobConfigurationQuery::allow_large_results].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::JobConfigurationQuery;
    /// use wkt::BoolValue;
    /// let x = JobConfigurationQuery::new().set_allow_large_results(BoolValue::default()/* use setters */);
    /// ```
    pub fn set_allow_large_results<T>(mut self, v: T) -> Self
    where
        T: std::convert::Into<wkt::BoolValue>,
    {
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_allow_large_results(v);
        self.job_config.query = Some(q);
        self.force_job_path = true;
        self
    }

    /// Sets the value of [clustering][crate::model::JobConfigurationQuery::clustering].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::JobConfigurationQuery;
    /// use google_cloud_bigquery_v2::model::Clustering;
    /// let x = JobConfigurationQuery::new().set_clustering(Clustering::default()/* use setters */);
    /// ```
    pub fn set_clustering<T>(mut self, v: T) -> Self
    where
        T: std::convert::Into<google_cloud_bigquery_v2::model::Clustering>,
    {
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_clustering(v);
        self.job_config.query = Some(q);
        self.force_job_path = true;
        self
    }

    /// Sets the value of [connection_properties][crate::model::QueryRequest::connection_properties].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::QueryRequest;
    /// use google_cloud_bigquery_v2::model::ConnectionProperty;
    /// let x = QueryRequest::new()
    /// .set_connection_properties([
    /// ConnectionProperty::default()/* use setters */,
    /// ConnectionProperty::default()/* use (different) setters */,
    /// ]);
    /// ```
    pub fn set_connection_properties<T, V>(mut self, v: T) -> Self
    where
        T: std::iter::IntoIterator<Item = V>,
        V: std::convert::Into<google_cloud_bigquery_v2::model::ConnectionProperty>,
    {
        let val: Vec<google_cloud_bigquery_v2::model::ConnectionProperty> =
            v.into_iter().map(|i| i.into()).collect();
        self.query_request = self.query_request.set_connection_properties(val.clone());
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_connection_properties(val);
        self.job_config.query = Some(q);
        self
    }

    /// Sets the value of [continuous][crate::model::JobConfigurationQuery::continuous].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::JobConfigurationQuery;
    /// use wkt::BoolValue;
    /// let x = JobConfigurationQuery::new().set_continuous(BoolValue::default()/* use setters */);
    /// ```
    pub fn set_continuous<T>(mut self, v: T) -> Self
    where
        T: std::convert::Into<wkt::BoolValue>,
    {
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_continuous(v);
        self.job_config.query = Some(q);
        self.force_job_path = true;
        self
    }

    /// Sets the value of [create_disposition][crate::model::JobConfigurationQuery::create_disposition].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::JobConfigurationQuery;
    /// let x = JobConfigurationQuery::new().set_create_disposition("example");
    /// ```
    pub fn set_create_disposition<T: std::convert::Into<std::string::String>>(
        mut self,
        v: T,
    ) -> Self {
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_create_disposition(v);
        self.job_config.query = Some(q);
        self.force_job_path = true;
        self
    }

    /// Sets the value of [create_session][crate::model::QueryRequest::create_session].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::QueryRequest;
    /// use wkt::BoolValue;
    /// let x = QueryRequest::new().set_create_session(BoolValue::default()/* use setters */);
    /// ```
    pub fn set_create_session<T>(mut self, v: T) -> Self
    where
        T: std::convert::Into<wkt::BoolValue>,
    {
        let val: wkt::BoolValue = v.into();
        self.query_request = self.query_request.set_create_session(val.clone());
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_create_session(val);
        self.job_config.query = Some(q);
        self
    }

    /// Sets the value of [default_dataset][crate::model::QueryRequest::default_dataset].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::QueryRequest;
    /// use google_cloud_bigquery_v2::model::DatasetReference;
    /// let x = QueryRequest::new().set_default_dataset(DatasetReference::default()/* use setters */);
    /// ```
    pub fn set_default_dataset<T>(mut self, v: T) -> Self
    where
        T: std::convert::Into<google_cloud_bigquery_v2::model::DatasetReference>,
    {
        let val: google_cloud_bigquery_v2::model::DatasetReference = v.into();
        self.query_request = self.query_request.set_default_dataset(val.clone());
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_default_dataset(val);
        self.job_config.query = Some(q);
        self
    }

    /// Sets the value of [destination_encryption_configuration][crate::model::QueryRequest::destination_encryption_configuration].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::QueryRequest;
    /// use google_cloud_bigquery_v2::model::EncryptionConfiguration;
    /// let x = QueryRequest::new().set_destination_encryption_configuration(EncryptionConfiguration::default()/* use setters */);
    /// ```
    pub fn set_destination_encryption_configuration<T>(mut self, v: T) -> Self
    where
        T: std::convert::Into<google_cloud_bigquery_v2::model::EncryptionConfiguration>,
    {
        let val: google_cloud_bigquery_v2::model::EncryptionConfiguration = v.into();
        self.query_request = self
            .query_request
            .set_destination_encryption_configuration(val.clone());
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_destination_encryption_configuration(val);
        self.job_config.query = Some(q);
        self
    }

    /// Sets the value of [destination_table][crate::model::JobConfigurationQuery::destination_table].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::JobConfigurationQuery;
    /// use google_cloud_bigquery_v2::model::TableReference;
    /// let x = JobConfigurationQuery::new().set_destination_table(TableReference::default()/* use setters */);
    /// ```
    pub fn set_destination_table<T>(mut self, v: T) -> Self
    where
        T: std::convert::Into<google_cloud_bigquery_v2::model::TableReference>,
    {
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_destination_table(v);
        self.job_config.query = Some(q);
        self.force_job_path = true;
        self
    }

    /// Sets the value of [dry_run][crate::model::QueryRequest::dry_run].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::QueryRequest;
    /// let x = QueryRequest::new().set_dry_run(true);
    /// ```
    pub fn set_dry_run<T: std::convert::Into<bool>>(mut self, v: T) -> Self {
        let val: bool = v.into();
        self.query_request = self.query_request.set_dry_run(val.clone());
        self.job_config = self.job_config.set_dry_run(val);
        self
    }

    /// Sets the value of [external_table_definitions][crate::model::JobConfigurationQuery::external_table_definitions].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::JobConfigurationQuery;
    /// use google_cloud_bigquery_v2::model::ExternalDataConfiguration;
    /// let x = JobConfigurationQuery::new().set_external_table_definitions([
    /// ("key0", ExternalDataConfiguration::default()/* use setters */),
    /// ("key1", ExternalDataConfiguration::default()/* use (different) setters */),
    /// ]);
    /// ```
    pub fn set_external_table_definitions<T, K, V>(mut self, v: T) -> Self
    where
        T: std::iter::IntoIterator<Item = (K, V)>,
        K: std::convert::Into<std::string::String>,
        V: std::convert::Into<google_cloud_bigquery_v2::model::ExternalDataConfiguration>,
    {
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_external_table_definitions(v);
        self.job_config.query = Some(q);
        self.force_job_path = true;
        self
    }

    /// Sets the value of [flatten_results][crate::model::JobConfigurationQuery::flatten_results].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::JobConfigurationQuery;
    /// use wkt::BoolValue;
    /// let x = JobConfigurationQuery::new().set_flatten_results(BoolValue::default()/* use setters */);
    /// ```
    pub fn set_flatten_results<T>(mut self, v: T) -> Self
    where
        T: std::convert::Into<wkt::BoolValue>,
    {
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_flatten_results(v);
        self.job_config.query = Some(q);
        self.force_job_path = true;
        self
    }

    /// Sets the value of [format_options][crate::model::QueryRequest::format_options].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::QueryRequest;
    /// use google_cloud_bigquery_v2::model::DataFormatOptions;
    /// let x = QueryRequest::new().set_format_options(DataFormatOptions::default()/* use setters */);
    /// ```
    pub fn set_format_options<T>(mut self, v: T) -> Self
    where
        T: std::convert::Into<google_cloud_bigquery_v2::model::DataFormatOptions>,
    {
        self.query_request = self.query_request.set_format_options(v);
        self
    }

    /// Sets the value of [job_creation_mode][crate::model::QueryRequest::job_creation_mode].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::QueryRequest;
    /// use google_cloud_bigquery_v2::model::query_request::JobCreationMode;
    /// let x0 = QueryRequest::new().set_job_creation_mode(JobCreationMode::JobCreationRequired);
    /// let x1 = QueryRequest::new().set_job_creation_mode(JobCreationMode::JobCreationOptional);
    /// ```
    pub fn set_job_creation_mode<
        T: std::convert::Into<google_cloud_bigquery_v2::model::query_request::JobCreationMode>,
    >(
        mut self,
        v: T,
    ) -> Self {
        self.query_request = self.query_request.set_job_creation_mode(v);
        self
    }

    /// Sets the value of [job_timeout_ms][crate::model::QueryRequest::job_timeout_ms].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::QueryRequest;
    /// let x = QueryRequest::new().set_job_timeout_ms(42);
    /// ```
    pub fn set_job_timeout_ms<T>(mut self, v: T) -> Self
    where
        T: std::convert::Into<i64>,
    {
        let val: i64 = v.into();
        self.query_request = self.query_request.set_job_timeout_ms(val.clone());
        self.job_config = self.job_config.set_job_timeout_ms(val);
        self
    }

    /// Sets the value of [labels][crate::model::QueryRequest::labels].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::QueryRequest;
    /// let x = QueryRequest::new().set_labels([
    /// ("key0", "abc"),
    /// ("key1", "xyz"),
    /// ]);
    /// ```
    pub fn set_labels<T, K, V>(mut self, v: T) -> Self
    where
        T: std::iter::IntoIterator<Item = (K, V)>,
        K: std::convert::Into<std::string::String>,
        V: std::convert::Into<std::string::String>,
    {
        let val: std::collections::HashMap<std::string::String, std::string::String> =
            v.into_iter().map(|(k, v)| (k.into(), v.into())).collect();
        self.query_request = self.query_request.set_labels(val.clone());
        self.job_config = self.job_config.set_labels(val);
        self
    }

    /// Sets the value of [location][crate::model::QueryRequest::location].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::QueryRequest;
    /// let x = QueryRequest::new().set_location("example");
    /// ```
    pub fn set_location<T: std::convert::Into<std::string::String>>(mut self, v: T) -> Self {
        self.query_request = self.query_request.set_location(v);
        self
    }

    /// Sets the value of [max_results][crate::model::QueryRequest::max_results].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::QueryRequest;
    /// use wkt::UInt32Value;
    /// let x = QueryRequest::new().set_max_results(UInt32Value::default()/* use setters */);
    /// ```
    pub fn set_max_results<T>(mut self, v: T) -> Self
    where
        T: std::convert::Into<wkt::UInt32Value>,
    {
        self.query_request = self.query_request.set_max_results(v);
        self
    }

    /// Sets the value of [max_slots][crate::model::QueryRequest::max_slots].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::QueryRequest;
    /// let x = QueryRequest::new().set_max_slots(42);
    /// ```
    pub fn set_max_slots<T>(mut self, v: T) -> Self
    where
        T: std::convert::Into<i32>,
    {
        let val: i32 = v.into();
        self.query_request = self.query_request.set_max_slots(val.clone());
        self.job_config = self.job_config.set_max_slots(val);
        self
    }

    /// Sets the value of [maximum_bytes_billed][crate::model::QueryRequest::maximum_bytes_billed].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::QueryRequest;
    /// use wkt::Int64Value;
    /// let x = QueryRequest::new().set_maximum_bytes_billed(Int64Value::default()/* use setters */);
    /// ```
    pub fn set_maximum_bytes_billed<T>(mut self, v: T) -> Self
    where
        T: std::convert::Into<wkt::Int64Value>,
    {
        let val: wkt::Int64Value = v.into();
        self.query_request = self.query_request.set_maximum_bytes_billed(val.clone());
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_maximum_bytes_billed(val);
        self.job_config.query = Some(q);
        self
    }

    /// Sets or clears the value of [allow_large_results][crate::model::JobConfigurationQuery::allow_large_results].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::JobConfigurationQuery;
    /// use wkt::BoolValue;
    /// let x = JobConfigurationQuery::new().set_or_clear_allow_large_results(Some(BoolValue::default()/* use setters */));
    /// let x = JobConfigurationQuery::new().set_or_clear_allow_large_results(None::<BoolValue>);
    /// ```
    pub fn set_or_clear_allow_large_results<T>(mut self, v: std::option::Option<T>) -> Self
    where
        T: std::convert::Into<wkt::BoolValue>,
    {
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_or_clear_allow_large_results(v);
        self.job_config.query = Some(q);
        self.force_job_path = true;
        self
    }

    /// Sets or clears the value of [clustering][crate::model::JobConfigurationQuery::clustering].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::JobConfigurationQuery;
    /// use google_cloud_bigquery_v2::model::Clustering;
    /// let x = JobConfigurationQuery::new().set_or_clear_clustering(Some(Clustering::default()/* use setters */));
    /// let x = JobConfigurationQuery::new().set_or_clear_clustering(None::<Clustering>);
    /// ```
    pub fn set_or_clear_clustering<T>(mut self, v: std::option::Option<T>) -> Self
    where
        T: std::convert::Into<google_cloud_bigquery_v2::model::Clustering>,
    {
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_or_clear_clustering(v);
        self.job_config.query = Some(q);
        self.force_job_path = true;
        self
    }

    /// Sets or clears the value of [continuous][crate::model::JobConfigurationQuery::continuous].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::JobConfigurationQuery;
    /// use wkt::BoolValue;
    /// let x = JobConfigurationQuery::new().set_or_clear_continuous(Some(BoolValue::default()/* use setters */));
    /// let x = JobConfigurationQuery::new().set_or_clear_continuous(None::<BoolValue>);
    /// ```
    pub fn set_or_clear_continuous<T>(mut self, v: std::option::Option<T>) -> Self
    where
        T: std::convert::Into<wkt::BoolValue>,
    {
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_or_clear_continuous(v);
        self.job_config.query = Some(q);
        self.force_job_path = true;
        self
    }

    /// Sets or clears the value of [create_session][crate::model::QueryRequest::create_session].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::QueryRequest;
    /// use wkt::BoolValue;
    /// let x = QueryRequest::new().set_or_clear_create_session(Some(BoolValue::default()/* use setters */));
    /// let x = QueryRequest::new().set_or_clear_create_session(None::<BoolValue>);
    /// ```
    pub fn set_or_clear_create_session<T>(mut self, v: std::option::Option<T>) -> Self
    where
        T: std::convert::Into<wkt::BoolValue>,
    {
        let val: Option<wkt::BoolValue> = v.map(|x| x.into());
        self.query_request = self.query_request.set_or_clear_create_session(val.clone());
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_or_clear_create_session(val);
        self.job_config.query = Some(q);
        self
    }

    /// Sets or clears the value of [default_dataset][crate::model::QueryRequest::default_dataset].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::QueryRequest;
    /// use google_cloud_bigquery_v2::model::DatasetReference;
    /// let x = QueryRequest::new().set_or_clear_default_dataset(Some(DatasetReference::default()/* use setters */));
    /// let x = QueryRequest::new().set_or_clear_default_dataset(None::<DatasetReference>);
    /// ```
    pub fn set_or_clear_default_dataset<T>(mut self, v: std::option::Option<T>) -> Self
    where
        T: std::convert::Into<google_cloud_bigquery_v2::model::DatasetReference>,
    {
        let val: Option<google_cloud_bigquery_v2::model::DatasetReference> = v.map(|x| x.into());
        self.query_request = self.query_request.set_or_clear_default_dataset(val.clone());
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_or_clear_default_dataset(val);
        self.job_config.query = Some(q);
        self
    }

    /// Sets or clears the value of [destination_encryption_configuration][crate::model::QueryRequest::destination_encryption_configuration].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::QueryRequest;
    /// use google_cloud_bigquery_v2::model::EncryptionConfiguration;
    /// let x = QueryRequest::new().set_or_clear_destination_encryption_configuration(Some(EncryptionConfiguration::default()/* use setters */));
    /// let x = QueryRequest::new().set_or_clear_destination_encryption_configuration(None::<EncryptionConfiguration>);
    /// ```
    pub fn set_or_clear_destination_encryption_configuration<T>(
        mut self,
        v: std::option::Option<T>,
    ) -> Self
    where
        T: std::convert::Into<google_cloud_bigquery_v2::model::EncryptionConfiguration>,
    {
        let val: Option<google_cloud_bigquery_v2::model::EncryptionConfiguration> =
            v.map(|x| x.into());
        self.query_request = self
            .query_request
            .set_or_clear_destination_encryption_configuration(val.clone());
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_or_clear_destination_encryption_configuration(val);
        self.job_config.query = Some(q);
        self
    }

    /// Sets or clears the value of [destination_table][crate::model::JobConfigurationQuery::destination_table].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::JobConfigurationQuery;
    /// use google_cloud_bigquery_v2::model::TableReference;
    /// let x = JobConfigurationQuery::new().set_or_clear_destination_table(Some(TableReference::default()/* use setters */));
    /// let x = JobConfigurationQuery::new().set_or_clear_destination_table(None::<TableReference>);
    /// ```
    pub fn set_or_clear_destination_table<T>(mut self, v: std::option::Option<T>) -> Self
    where
        T: std::convert::Into<google_cloud_bigquery_v2::model::TableReference>,
    {
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_or_clear_destination_table(v);
        self.job_config.query = Some(q);
        self.force_job_path = true;
        self
    }

    /// Sets or clears the value of [dry_run][crate::model::JobConfiguration::dry_run].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::JobConfiguration;
    /// use wkt::BoolValue;
    /// let x = JobConfiguration::new().set_or_clear_dry_run(Some(BoolValue::default()/* use setters */));
    /// let x = JobConfiguration::new().set_or_clear_dry_run(None::<BoolValue>);
    /// ```
    pub fn set_or_clear_dry_run<T>(mut self, v: std::option::Option<T>) -> Self
    where
        T: std::convert::Into<wkt::BoolValue>,
    {
        self.job_config = self.job_config.set_or_clear_dry_run(v);
        self.force_job_path = true;
        self
    }

    /// Sets or clears the value of [flatten_results][crate::model::JobConfigurationQuery::flatten_results].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::JobConfigurationQuery;
    /// use wkt::BoolValue;
    /// let x = JobConfigurationQuery::new().set_or_clear_flatten_results(Some(BoolValue::default()/* use setters */));
    /// let x = JobConfigurationQuery::new().set_or_clear_flatten_results(None::<BoolValue>);
    /// ```
    pub fn set_or_clear_flatten_results<T>(mut self, v: std::option::Option<T>) -> Self
    where
        T: std::convert::Into<wkt::BoolValue>,
    {
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_or_clear_flatten_results(v);
        self.job_config.query = Some(q);
        self.force_job_path = true;
        self
    }

    /// Sets or clears the value of [format_options][crate::model::QueryRequest::format_options].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::QueryRequest;
    /// use google_cloud_bigquery_v2::model::DataFormatOptions;
    /// let x = QueryRequest::new().set_or_clear_format_options(Some(DataFormatOptions::default()/* use setters */));
    /// let x = QueryRequest::new().set_or_clear_format_options(None::<DataFormatOptions>);
    /// ```
    pub fn set_or_clear_format_options<T>(mut self, v: std::option::Option<T>) -> Self
    where
        T: std::convert::Into<google_cloud_bigquery_v2::model::DataFormatOptions>,
    {
        self.query_request = self.query_request.set_or_clear_format_options(v);
        self
    }

    /// Sets or clears the value of [job_timeout_ms][crate::model::QueryRequest::job_timeout_ms].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::QueryRequest;
    /// let x = QueryRequest::new().set_or_clear_job_timeout_ms(Some(42));
    /// let x = QueryRequest::new().set_or_clear_job_timeout_ms(None::<i32>);
    /// ```
    pub fn set_or_clear_job_timeout_ms<T>(mut self, v: std::option::Option<T>) -> Self
    where
        T: std::convert::Into<i64>,
    {
        let val: Option<i64> = v.map(|x| x.into());
        self.query_request = self.query_request.set_or_clear_job_timeout_ms(val.clone());
        self.job_config = self.job_config.set_or_clear_job_timeout_ms(val);
        self
    }

    /// Sets or clears the value of [max_results][crate::model::QueryRequest::max_results].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::QueryRequest;
    /// use wkt::UInt32Value;
    /// let x = QueryRequest::new().set_or_clear_max_results(Some(UInt32Value::default()/* use setters */));
    /// let x = QueryRequest::new().set_or_clear_max_results(None::<UInt32Value>);
    /// ```
    pub fn set_or_clear_max_results<T>(mut self, v: std::option::Option<T>) -> Self
    where
        T: std::convert::Into<wkt::UInt32Value>,
    {
        self.query_request = self.query_request.set_or_clear_max_results(v);
        self
    }

    /// Sets or clears the value of [max_slots][crate::model::QueryRequest::max_slots].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::QueryRequest;
    /// let x = QueryRequest::new().set_or_clear_max_slots(Some(42));
    /// let x = QueryRequest::new().set_or_clear_max_slots(None::<i32>);
    /// ```
    pub fn set_or_clear_max_slots<T>(mut self, v: std::option::Option<T>) -> Self
    where
        T: std::convert::Into<i32>,
    {
        let val: Option<i32> = v.map(|x| x.into());
        self.query_request = self.query_request.set_or_clear_max_slots(val.clone());
        self.job_config = self.job_config.set_or_clear_max_slots(val);
        self
    }

    /// Sets or clears the value of [maximum_bytes_billed][crate::model::QueryRequest::maximum_bytes_billed].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::QueryRequest;
    /// use wkt::Int64Value;
    /// let x = QueryRequest::new().set_or_clear_maximum_bytes_billed(Some(Int64Value::default()/* use setters */));
    /// let x = QueryRequest::new().set_or_clear_maximum_bytes_billed(None::<Int64Value>);
    /// ```
    pub fn set_or_clear_maximum_bytes_billed<T>(mut self, v: std::option::Option<T>) -> Self
    where
        T: std::convert::Into<wkt::Int64Value>,
    {
        let val: Option<wkt::Int64Value> = v.map(|x| x.into());
        self.query_request = self
            .query_request
            .set_or_clear_maximum_bytes_billed(val.clone());
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_or_clear_maximum_bytes_billed(val);
        self.job_config.query = Some(q);
        self
    }

    /// Sets or clears the value of [query][crate::model::JobConfiguration::query].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::JobConfiguration;
    /// use google_cloud_bigquery_v2::model::JobConfigurationQuery;
    /// let x = JobConfiguration::new().set_or_clear_query(Some(JobConfigurationQuery::default()/* use setters */));
    /// let x = JobConfiguration::new().set_or_clear_query(None::<JobConfigurationQuery>);
    /// ```
    pub fn set_or_clear_query<T>(mut self, v: std::option::Option<T>) -> Self
    where
        T: std::convert::Into<google_cloud_bigquery_v2::model::JobConfigurationQuery>,
    {
        self.job_config = self.job_config.set_or_clear_query(v);
        self.force_job_path = true;
        self
    }

    /// Sets or clears the value of [range_partitioning][crate::model::JobConfigurationQuery::range_partitioning].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::JobConfigurationQuery;
    /// use google_cloud_bigquery_v2::model::RangePartitioning;
    /// let x = JobConfigurationQuery::new().set_or_clear_range_partitioning(Some(RangePartitioning::default()/* use setters */));
    /// let x = JobConfigurationQuery::new().set_or_clear_range_partitioning(None::<RangePartitioning>);
    /// ```
    pub fn set_or_clear_range_partitioning<T>(mut self, v: std::option::Option<T>) -> Self
    where
        T: std::convert::Into<google_cloud_bigquery_v2::model::RangePartitioning>,
    {
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_or_clear_range_partitioning(v);
        self.job_config.query = Some(q);
        self.force_job_path = true;
        self
    }

    /// Sets or clears the value of [reservation][crate::model::QueryRequest::reservation].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::QueryRequest;
    /// let x = QueryRequest::new().set_or_clear_reservation(Some("example"));
    /// let x = QueryRequest::new().set_or_clear_reservation(None::<String>);
    /// ```
    pub fn set_or_clear_reservation<T>(mut self, v: std::option::Option<T>) -> Self
    where
        T: std::convert::Into<std::string::String>,
    {
        let val: Option<std::string::String> = v.map(|x| x.into());
        self.query_request = self.query_request.set_or_clear_reservation(val.clone());
        self.job_config = self.job_config.set_or_clear_reservation(val);
        self
    }

    /// Sets or clears the value of [script_options][crate::model::JobConfigurationQuery::script_options].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::JobConfigurationQuery;
    /// use google_cloud_bigquery_v2::model::ScriptOptions;
    /// let x = JobConfigurationQuery::new().set_or_clear_script_options(Some(ScriptOptions::default()/* use setters */));
    /// let x = JobConfigurationQuery::new().set_or_clear_script_options(None::<ScriptOptions>);
    /// ```
    pub fn set_or_clear_script_options<T>(mut self, v: std::option::Option<T>) -> Self
    where
        T: std::convert::Into<google_cloud_bigquery_v2::model::ScriptOptions>,
    {
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_or_clear_script_options(v);
        self.job_config.query = Some(q);
        self.force_job_path = true;
        self
    }

    /// Sets or clears the value of [system_variables][crate::model::JobConfigurationQuery::system_variables].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::JobConfigurationQuery;
    /// use google_cloud_bigquery_v2::model::SystemVariables;
    /// let x = JobConfigurationQuery::new().set_or_clear_system_variables(Some(SystemVariables::default()/* use setters */));
    /// let x = JobConfigurationQuery::new().set_or_clear_system_variables(None::<SystemVariables>);
    /// ```
    pub fn set_or_clear_system_variables<T>(mut self, v: std::option::Option<T>) -> Self
    where
        T: std::convert::Into<google_cloud_bigquery_v2::model::SystemVariables>,
    {
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_or_clear_system_variables(v);
        self.job_config.query = Some(q);
        self.force_job_path = true;
        self
    }

    /// Sets or clears the value of [time_partitioning][crate::model::JobConfigurationQuery::time_partitioning].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::JobConfigurationQuery;
    /// use google_cloud_bigquery_v2::model::TimePartitioning;
    /// let x = JobConfigurationQuery::new().set_or_clear_time_partitioning(Some(TimePartitioning::default()/* use setters */));
    /// let x = JobConfigurationQuery::new().set_or_clear_time_partitioning(None::<TimePartitioning>);
    /// ```
    pub fn set_or_clear_time_partitioning<T>(mut self, v: std::option::Option<T>) -> Self
    where
        T: std::convert::Into<google_cloud_bigquery_v2::model::TimePartitioning>,
    {
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_or_clear_time_partitioning(v);
        self.job_config.query = Some(q);
        self.force_job_path = true;
        self
    }

    /// Sets or clears the value of [timeout_ms][crate::model::QueryRequest::timeout_ms].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::QueryRequest;
    /// use wkt::UInt32Value;
    /// let x = QueryRequest::new().set_or_clear_timeout_ms(Some(UInt32Value::default()/* use setters */));
    /// let x = QueryRequest::new().set_or_clear_timeout_ms(None::<UInt32Value>);
    /// ```
    pub fn set_or_clear_timeout_ms<T>(mut self, v: std::option::Option<T>) -> Self
    where
        T: std::convert::Into<wkt::UInt32Value>,
    {
        self.query_request = self.query_request.set_or_clear_timeout_ms(v);
        self
    }

    /// Sets or clears the value of [use_legacy_sql][crate::model::QueryRequest::use_legacy_sql].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::QueryRequest;
    /// use wkt::BoolValue;
    /// let x = QueryRequest::new().set_or_clear_use_legacy_sql(Some(BoolValue::default()/* use setters */));
    /// let x = QueryRequest::new().set_or_clear_use_legacy_sql(None::<BoolValue>);
    /// ```
    pub fn set_or_clear_use_legacy_sql<T>(mut self, v: std::option::Option<T>) -> Self
    where
        T: std::convert::Into<wkt::BoolValue>,
    {
        let val: Option<wkt::BoolValue> = v.map(|x| x.into());
        self.query_request = self.query_request.set_or_clear_use_legacy_sql(val.clone());
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_or_clear_use_legacy_sql(val);
        self.job_config.query = Some(q);
        self
    }

    /// Sets or clears the value of [use_query_cache][crate::model::QueryRequest::use_query_cache].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::QueryRequest;
    /// use wkt::BoolValue;
    /// let x = QueryRequest::new().set_or_clear_use_query_cache(Some(BoolValue::default()/* use setters */));
    /// let x = QueryRequest::new().set_or_clear_use_query_cache(None::<BoolValue>);
    /// ```
    pub fn set_or_clear_use_query_cache<T>(mut self, v: std::option::Option<T>) -> Self
    where
        T: std::convert::Into<wkt::BoolValue>,
    {
        let val: Option<wkt::BoolValue> = v.map(|x| x.into());
        self.query_request = self.query_request.set_or_clear_use_query_cache(val.clone());
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_or_clear_use_query_cache(val);
        self.job_config.query = Some(q);
        self
    }

    /// Sets the value of [parameter_mode][crate::model::QueryRequest::parameter_mode].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::QueryRequest;
    /// let x = QueryRequest::new().set_parameter_mode("example");
    /// ```
    pub fn set_parameter_mode<T: std::convert::Into<std::string::String>>(mut self, v: T) -> Self {
        let val: std::string::String = v.into();
        self.query_request = self.query_request.set_parameter_mode(val.clone());
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_parameter_mode(val);
        self.job_config.query = Some(q);
        self
    }

    /// Sets the value of [priority][crate::model::JobConfigurationQuery::priority].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::JobConfigurationQuery;
    /// let x = JobConfigurationQuery::new().set_priority("example");
    /// ```
    pub fn set_priority<T: std::convert::Into<std::string::String>>(mut self, v: T) -> Self {
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_priority(v);
        self.job_config.query = Some(q);
        self.force_job_path = true;
        self
    }

    /// Sets the value of [query_parameters][crate::model::QueryRequest::query_parameters].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::QueryRequest;
    /// use google_cloud_bigquery_v2::model::QueryParameter;
    /// let x = QueryRequest::new()
    /// .set_query_parameters([
    /// QueryParameter::default()/* use setters */,
    /// QueryParameter::default()/* use (different) setters */,
    /// ]);
    /// ```
    pub fn set_query_parameters<T, V>(mut self, v: T) -> Self
    where
        T: std::iter::IntoIterator<Item = V>,
        V: std::convert::Into<google_cloud_bigquery_v2::model::QueryParameter>,
    {
        let val: Vec<google_cloud_bigquery_v2::model::QueryParameter> =
            v.into_iter().map(|i| i.into()).collect();
        self.query_request = self.query_request.set_query_parameters(val.clone());
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_query_parameters(val);
        self.job_config.query = Some(q);
        self
    }

    /// Sets the value of [range_partitioning][crate::model::JobConfigurationQuery::range_partitioning].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::JobConfigurationQuery;
    /// use google_cloud_bigquery_v2::model::RangePartitioning;
    /// let x = JobConfigurationQuery::new().set_range_partitioning(RangePartitioning::default()/* use setters */);
    /// ```
    pub fn set_range_partitioning<T>(mut self, v: T) -> Self
    where
        T: std::convert::Into<google_cloud_bigquery_v2::model::RangePartitioning>,
    {
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_range_partitioning(v);
        self.job_config.query = Some(q);
        self.force_job_path = true;
        self
    }

    /// Sets the value of [request_id][crate::model::QueryRequest::request_id].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::QueryRequest;
    /// let x = QueryRequest::new().set_request_id("example");
    /// ```
    pub fn set_request_id<T: std::convert::Into<std::string::String>>(mut self, v: T) -> Self {
        self.query_request = self.query_request.set_request_id(v);
        self
    }

    /// Sets the value of [reservation][crate::model::QueryRequest::reservation].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::QueryRequest;
    /// let x = QueryRequest::new().set_reservation("example");
    /// ```
    pub fn set_reservation<T>(mut self, v: T) -> Self
    where
        T: std::convert::Into<std::string::String>,
    {
        let val: std::string::String = v.into();
        self.query_request = self.query_request.set_reservation(val.clone());
        self.job_config = self.job_config.set_reservation(val);
        self
    }

    /// Sets the value of [schema_update_options][crate::model::JobConfigurationQuery::schema_update_options].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::JobConfigurationQuery;
    /// let x = JobConfigurationQuery::new().set_schema_update_options(["a", "b", "c"]);
    /// ```
    pub fn set_schema_update_options<T, V>(mut self, v: T) -> Self
    where
        T: std::iter::IntoIterator<Item = V>,
        V: std::convert::Into<std::string::String>,
    {
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_schema_update_options(v);
        self.job_config.query = Some(q);
        self.force_job_path = true;
        self
    }

    /// Sets the value of [script_options][crate::model::JobConfigurationQuery::script_options].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::JobConfigurationQuery;
    /// use google_cloud_bigquery_v2::model::ScriptOptions;
    /// let x = JobConfigurationQuery::new().set_script_options(ScriptOptions::default()/* use setters */);
    /// ```
    pub fn set_script_options<T>(mut self, v: T) -> Self
    where
        T: std::convert::Into<google_cloud_bigquery_v2::model::ScriptOptions>,
    {
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_script_options(v);
        self.job_config.query = Some(q);
        self.force_job_path = true;
        self
    }

    /// Sets the value of [system_variables][crate::model::JobConfigurationQuery::system_variables].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::JobConfigurationQuery;
    /// use google_cloud_bigquery_v2::model::SystemVariables;
    /// let x = JobConfigurationQuery::new().set_system_variables(SystemVariables::default()/* use setters */);
    /// ```
    pub fn set_system_variables<T>(mut self, v: T) -> Self
    where
        T: std::convert::Into<google_cloud_bigquery_v2::model::SystemVariables>,
    {
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_system_variables(v);
        self.job_config.query = Some(q);
        self.force_job_path = true;
        self
    }

    /// Sets the value of [time_partitioning][crate::model::JobConfigurationQuery::time_partitioning].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::JobConfigurationQuery;
    /// use google_cloud_bigquery_v2::model::TimePartitioning;
    /// let x = JobConfigurationQuery::new().set_time_partitioning(TimePartitioning::default()/* use setters */);
    /// ```
    pub fn set_time_partitioning<T>(mut self, v: T) -> Self
    where
        T: std::convert::Into<google_cloud_bigquery_v2::model::TimePartitioning>,
    {
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_time_partitioning(v);
        self.job_config.query = Some(q);
        self.force_job_path = true;
        self
    }

    /// Sets the value of [timeout_ms][crate::model::QueryRequest::timeout_ms].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::QueryRequest;
    /// use wkt::UInt32Value;
    /// let x = QueryRequest::new().set_timeout_ms(UInt32Value::default()/* use setters */);
    /// ```
    pub fn set_timeout_ms<T>(mut self, v: T) -> Self
    where
        T: std::convert::Into<wkt::UInt32Value>,
    {
        self.query_request = self.query_request.set_timeout_ms(v);
        self
    }

    /// Sets the value of [use_legacy_sql][crate::model::QueryRequest::use_legacy_sql].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::QueryRequest;
    /// use wkt::BoolValue;
    /// let x = QueryRequest::new().set_use_legacy_sql(BoolValue::default()/* use setters */);
    /// ```
    pub fn set_use_legacy_sql<T>(mut self, v: T) -> Self
    where
        T: std::convert::Into<wkt::BoolValue>,
    {
        let val: wkt::BoolValue = v.into();
        self.query_request = self.query_request.set_use_legacy_sql(val.clone());
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_use_legacy_sql(val);
        self.job_config.query = Some(q);
        self
    }

    /// Sets the value of [use_query_cache][crate::model::QueryRequest::use_query_cache].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::QueryRequest;
    /// use wkt::BoolValue;
    /// let x = QueryRequest::new().set_use_query_cache(BoolValue::default()/* use setters */);
    /// ```
    pub fn set_use_query_cache<T>(mut self, v: T) -> Self
    where
        T: std::convert::Into<wkt::BoolValue>,
    {
        let val: wkt::BoolValue = v.into();
        self.query_request = self.query_request.set_use_query_cache(val.clone());
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_use_query_cache(val);
        self.job_config.query = Some(q);
        self
    }

    /// Sets the value of [user_defined_function_resources][crate::model::JobConfigurationQuery::user_defined_function_resources].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::JobConfigurationQuery;
    /// use google_cloud_bigquery_v2::model::UserDefinedFunctionResource;
    /// let x = JobConfigurationQuery::new()
    /// .set_user_defined_function_resources([
    /// UserDefinedFunctionResource::default()/* use setters */,
    /// UserDefinedFunctionResource::default()/* use (different) setters */,
    /// ]);
    /// ```
    pub fn set_user_defined_function_resources<T, V>(mut self, v: T) -> Self
    where
        T: std::iter::IntoIterator<Item = V>,
        V: std::convert::Into<google_cloud_bigquery_v2::model::UserDefinedFunctionResource>,
    {
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_user_defined_function_resources(v);
        self.job_config.query = Some(q);
        self.force_job_path = true;
        self
    }

    /// Sets the value of [write_disposition][crate::model::JobConfigurationQuery::write_disposition].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::JobConfigurationQuery;
    /// let x = JobConfigurationQuery::new().set_write_disposition("example");
    /// ```
    pub fn set_write_disposition<T: std::convert::Into<std::string::String>>(
        mut self,
        v: T,
    ) -> Self {
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_write_disposition(v);
        self.job_config.query = Some(q);
        self.force_job_path = true;
        self
    }

    /// Sets the value of [write_incremental_results][crate::model::QueryRequest::write_incremental_results].
    ///
    /// # Example
    /// ```ignore,no_run
    /// # use google_cloud_bigquery_v2::model::QueryRequest;
    /// let x = QueryRequest::new().set_write_incremental_results(true);
    /// ```
    pub fn set_write_incremental_results<T: std::convert::Into<bool>>(mut self, v: T) -> Self {
        let val: bool = v.into();
        self.query_request = self
            .query_request
            .set_write_incremental_results(val.clone());
        let mut q = self.job_config.query.take().unwrap_or_default();
        q = q.set_write_incremental_results(val);
        self.job_config.query = Some(q);
        self
    }
}
