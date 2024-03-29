//! Basic types for describing table data and metadata

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Representation of the table protocol.
///
/// Protocol versioning will allow servers to exclude older clients that are
/// missing features required to correctly interpret their response if the
/// Delta Sharing Protocol evolves in the future. The protocol version will be
/// increased whenever non-backwards-compatible changes are made to the
/// protocol. When a client is running an unsupported protocol version, it
/// should show an error message instructing the user to upgrade to a newer
/// version of their client.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Protocol {
    /// The minimum version of the protocol that the client must support.
    min_reader_version: u32,
}

impl Protocol {
    /// Retrieve the minimum version of the protocol that the client must
    /// implement to read this table.
    pub fn min_reader_version(&self) -> u32 {
        self.min_reader_version
    }
}

/// Configure a protocol action.
pub struct ProtocolBuilder {
    min_reader_version: u32,
}

impl ProtocolBuilder {
    /// Initialize a new ProtocolBuilder.
    pub fn new() -> Self {
        Self {
            min_reader_version: 1,
        }
    }

    /// Set the minimum version of the protocol that the client must support.
    ///
    /// # Example
    /// ```
    /// use delta_sharing_server::protocol::action::ProtocolBuilder;
    ///
    /// let protocol = ProtocolBuilder::new()
    ///    .min_reader_version(2)
    ///    .build();
    ///
    /// assert_eq!(protocol.min_reader_version(), 2);
    /// ```
    pub fn min_reader_version(mut self, min_reader_version: u32) -> Self {
        self.min_reader_version = min_reader_version;
        self
    }

    /// Build the configured protocol action.
    pub fn build(self) -> Protocol {
        Protocol {
            min_reader_version: self.min_reader_version,
        }
    }
}

impl Default for ProtocolBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Representation of the table metadata.
///
/// The metadata of a table contains all the information required to correctly
/// interpret the data files of the table.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    format: FileFormat,
    schema_string: String,
    partition_columns: Vec<String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    configuration: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    num_files: Option<u64>,
}

impl Metadata {
    /// Retrieve the unique table identifier.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Retrieve the table name provided by the user.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Retrieve the table description provided by the user.
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    /// Retrieve the specification of the table format.
    pub fn format(&self) -> &FileFormat {
        &self.format
    }

    /// Retrieve the schema of the table, serialized as a string.
    pub fn schema_string(&self) -> &str {
        &self.schema_string
    }

    /// Retrieve an array of column names that are used to partition the table.
    pub fn partition_columns(&self) -> &[String] {
        &self.partition_columns
    }

    /// Retrieve a map containing configuration options for the table.
    pub fn configuration(&self) -> &HashMap<String, String> {
        &self.configuration
    }

    /// Retrieve the version of the table this metadata corresponds to.
    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }

    /// Retrieve the size of the table in bytes.
    pub fn size(&self) -> Option<u64> {
        self.size
    }

    /// Retrieve the number of files in the table.
    pub fn num_files(&self) -> Option<u64> {
        self.num_files
    }
}

/// Representation of the table format.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FileFormat {
    /// The format of the data files backing the shared table.
    pub provider: String,
}

/// Build a new Metadata action.
pub struct MetadataBuilder {
    id: String,
    name: Option<String>,
    description: Option<String>,
    format: FileFormat,
    schema_string: String,
    partition_columns: Vec<String>,
    configuration: HashMap<String, String>,
    version: Option<String>,
    size: Option<u64>,
    num_files: Option<u64>,
}

impl MetadataBuilder {
    /// Initialize a new MetadataBuilder.
    pub fn new<S: Into<String>>(id: S, schema_string: S) -> Self {
        Self {
            id: id.into(),
            name: None,
            description: None,
            format: FileFormat {
                provider: "parquet".to_string(),
            },
            schema_string: schema_string.into(),
            partition_columns: vec![],
            configuration: HashMap::new(),
            version: None,
            size: None,
            num_files: None,
        }
    }

    /// Set the name of the table.
    pub fn name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    /// Set the description of the table.
    pub fn description(mut self, description: String) -> Self {
        self.description = Some(description);
        self
    }

    /// Set the format of the table.
    pub fn format(mut self, format: FileFormat) -> Self {
        self.format = format;
        self
    }

    /// Set the partition columns of the table.
    pub fn partition_columns(mut self, partition_columns: Vec<String>) -> Self {
        self.partition_columns = partition_columns;
        self
    }

    /// Set the configuration of the table.
    pub fn configuration(mut self, configuration: HashMap<String, String>) -> Self {
        self.configuration = configuration;
        self
    }

    /// Set the version of the table.
    pub fn version(mut self, version: String) -> Self {
        self.version = Some(version);
        self
    }

    /// Set the size of the table.
    pub fn size(mut self, size: u64) -> Self {
        self.size = Some(size);
        self
    }

    /// Set the number of files in the table.
    pub fn num_files(mut self, num_files: u64) -> Self {
        self.num_files = Some(num_files);
        self
    }

    /// Build the Metadata.
    pub fn build(self) -> Metadata {
        Metadata {
            id: self.id,
            name: self.name,
            description: self.description,
            format: self.format,
            schema_string: self.schema_string,
            partition_columns: self.partition_columns,
            configuration: self.configuration,
            version: self.version,
            size: self.size,
            num_files: self.num_files,
        }
    }
}

/// Representation of data that is part of a table.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct File {
    url: String,
    id: String,
    partition_values: HashMap<String, Option<String>>,
    size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    stats: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    timestamp: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expiration_timestamp: Option<String>,
}

impl File {
    /// An HTTPS url that a client can use to directly read the data file.
    pub fn url(&self) -> &str {
        self.url.as_ref()
    }

    /// A mutable HTTPS url that a client can use to directly read the data file.
    pub fn url_mut(&mut self) -> &mut String {
        &mut self.url
    }

    /// A unique identifier for the data file in the table.
    pub fn id(&self) -> &str {
        self.id.as_ref()
    }

    /// A map from partition column to value for this file in the table.
    pub fn partition_values(&self) -> &HashMap<String, Option<String>> {
        &self.partition_values
    }

    /// The size of this file in bytes.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Summary statistics about the data in this file.
    pub fn stats(&self) -> Option<&str> {
        self.stats.as_deref()
    }

    /// The table version associated with this file.
    pub fn version(&self) -> Option<u64> {
        self.version
    }

    /// The unix timestamp in milliseconds corresponding to the table version
    /// associated with this file.
    pub fn timestamp(&self) -> Option<&str> {
        self.timestamp.as_deref()
    }

    /// The unix timestamp in milliseconds corresponding to the expiration of
    /// the url associated with this file.
    pub fn expiration_timestamp(&self) -> Option<&str> {
        self.expiration_timestamp.as_deref()
    }
}

/// Build a new File action
pub struct FileBuilder {
    url: String,
    id: String,
    partition_values: HashMap<String, Option<String>>,
    size: Option<u64>,
    stats: Option<String>,
    version: Option<u64>,
    timestamp: Option<String>,
    expiration_timestamp: Option<String>,
}

impl FileBuilder {
    /// Initialize a new FileBuilder.
    pub fn new(url: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            id: id.into(),
            partition_values: HashMap::new(),
            size: None,
            stats: None,
            version: None,
            timestamp: None,
            expiration_timestamp: None,
        }
    }

    /// Set the partition values for this file.
    pub fn partition_values(mut self, partition_values: HashMap<String, Option<String>>) -> Self {
        self.partition_values = partition_values;
        self
    }

    /// Add a partition value for this file.
    pub fn add_partition_value(
        mut self,
        partition: impl Into<String>,
        value: Option<impl Into<String>>,
    ) -> Self {
        self.partition_values
            .insert(partition.into(), value.map(Into::into));
        self
    }

    /// Set the size of this file in bytes.
    pub fn size(mut self, size: u64) -> Self {
        self.size = Some(size);
        self
    }

    /// Set the statistics of this file.
    pub fn stats(mut self, stats: impl Into<String>) -> Self {
        self.stats = Some(stats.into());
        self
    }

    /// Set the version of this file.
    pub fn version(mut self, version: u64) -> Self {
        self.version = Some(version);
        self
    }

    /// Set the timestamp of this file.
    pub fn timestamp(mut self, ts: impl Into<String>) -> Self {
        self.timestamp = Some(ts.into());
        self
    }

    /// Set the expiration timestamp for the url belonging to this file.
    /// This is only relevant for urls that have been presigned.
    pub fn expiration_timestamp(mut self, ts: impl Into<String>) -> Self {
        self.expiration_timestamp = Some(ts.into());
        self
    }

    /// Build a File from the provided configuration.
    pub fn build(self) -> File {
        File {
            url: self.url,
            id: self.id,
            partition_values: self.partition_values,
            size: self.size.unwrap_or(0),
            stats: self.stats,
            version: self.version,
            timestamp: self.timestamp,
            expiration_timestamp: self.expiration_timestamp,
        }
    }
}

/// Representation of data that was added to a table.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Add {
    /// An HTTPS url that a client can use to directly read the data file.
    pub url: String,
    /// A unique identifier for the data file in the table.
    pub id: String,
    /// A map from partition column to value for this file in the table.
    pub partition_values: HashMap<String, Option<String>>,
    /// The size of the file in bytes.
    pub size: u64,
    /// Summary statistics about the data in this file.
    pub stats: Option<String>,
    /// The table version associated with this file.
    pub version: u64,
    /// The unix timestamp in milliseconds corresponding to the table version
    /// associated with this file.
    pub timestamp: String,
    /// The unix timestamp in milliseconds corresponding to the expiration of
    /// the url associated with this file.
    pub expiration_timestamp: Option<String>,
}

/// Initialize a new AddBuilder.
pub struct AddBuilder {
    url: String,
    id: String,
    partition_values: HashMap<String, Option<String>>,
    size: Option<u64>,
    stats: Option<String>,
    version: Option<u64>,
    timestamp: Option<String>,
    expiration_timestamp: Option<String>,
}

impl AddBuilder {
    /// Initialize a new AddBuilder.
    pub fn new(url: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            id: id.into(),
            partition_values: HashMap::new(),
            size: None,
            stats: None,
            version: None,
            timestamp: None,
            expiration_timestamp: None,
        }
    }

    /// Set the partition values for this file.
    pub fn partition_values(mut self, partition_values: HashMap<String, Option<String>>) -> Self {
        self.partition_values = partition_values;
        self
    }

    /// Add a partition value for this file.
    pub fn add_partition_value<S: Into<String>>(mut self, partition: S, value: Option<S>) -> Self {
        self.partition_values
            .insert(partition.into(), value.map(Into::into));
        self
    }

    /// Set file level statistics for this file.
    pub fn stats(mut self, stats: impl Into<String>) -> Self {
        self.stats = Some(stats.into());
        self
    }

    /// Expiration timestamp for the url associated with this file.
    pub fn expiration_timestamp(mut self, ts: impl Into<String>) -> Self {
        self.expiration_timestamp = Some(ts.into());
        self
    }

    /// Build an Add from the provided configuration.
    pub fn build(self) -> Add {
        Add {
            url: self.url,
            id: self.id,
            partition_values: self.partition_values,
            size: self.size.unwrap_or(0),
            stats: self.stats,
            version: self.version.unwrap_or(0),
            timestamp: self.timestamp.unwrap_or("0".to_owned()),
            expiration_timestamp: self.expiration_timestamp,
        }
    }
}

/// Representation of a data that has changed in the table.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Cdf {
    /// An HTTPS url that a client can use to directly read the data file.
    pub url: String,
    /// A unique identifier for the data file in the table.
    pub id: String,
    /// A map from partition column to value for this file in the table.
    pub partition_values: HashMap<String, Option<String>>,
    /// The size of the file in bytes.
    pub size: u64,
    /// Summary statistics about the data in this file.
    pub stats: Option<String>,
    /// The table version associated with this file.
    pub version: u64,
    /// The unix timestamp in milliseconds corresponding to the table version
    /// associated with this file.
    pub timestamp: String,
    /// The unix timestamp in milliseconds corresponding to the expiration of
    /// the url associated with this file.
    pub expiration_timestamp: Option<String>,
}

/// Initialize a new CdfBuilder.
pub struct CdfBuilder {
    url: String,
    id: String,
    partition_values: HashMap<String, Option<String>>,
    size: u64,
    stats: Option<String>,
    version: u64,
    timestamp: String,
    expiration_timestamp: Option<String>,
}

impl CdfBuilder {
    /// Initialize a new CdfBuilder.
    pub fn new<S: Into<String>>(url: S, id: S, size: u64, version: u64, timestamp: S) -> Self {
        Self {
            url: url.into(),
            id: id.into(),
            partition_values: HashMap::new(),
            size,
            stats: None,
            version,
            timestamp: timestamp.into(),
            expiration_timestamp: None,
        }
    }

    /// Set the partition values for this file.
    pub fn partition_values(mut self, partition_values: HashMap<String, Option<String>>) -> Self {
        self.partition_values = partition_values;
        self
    }

    /// Add a partition value for this file.
    pub fn add_partition_value<S: Into<String>>(mut self, partition: S, value: Option<S>) -> Self {
        self.partition_values
            .insert(partition.into(), value.map(Into::into));
        self
    }

    /// Set file level statistics for this file.
    pub fn stats(mut self, stats: impl Into<String>) -> Self {
        self.stats = Some(stats.into());
        self
    }

    /// Expiration timestamp for the url associated with this file.
    pub fn expiration_timestamp(mut self, ts: impl Into<String>) -> Self {
        self.expiration_timestamp = Some(ts.into());
        self
    }

    /// Build a Cdf from the provided configuration.
    pub fn build(self) -> Cdf {
        Cdf {
            url: self.url,
            id: self.id,
            partition_values: self.partition_values,
            size: self.size,
            stats: self.stats,
            version: self.version,
            timestamp: self.timestamp,
            expiration_timestamp: self.expiration_timestamp,
        }
    }
}

/// Representation of a data that has been removed from the table.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Remove {
    /// An HTTPS url that a client can use to directly read the data file.
    pub url: String,
    /// A unique identifier for the data file in the table.
    pub id: String,
    /// A map from partition column to value for this file in the table.
    pub partition_values: HashMap<String, Option<String>>,
    /// The size of the file in bytes.
    pub size: u64,
    /// Summary statistics about the data in this file.
    pub stats: Option<String>,
    /// The table version associated with this file.
    pub version: u64,
    /// The unix timestamp in milliseconds corresponding to the table version
    /// associated with this file.
    pub timestamp: String,
    /// The unix timestamp in milliseconds corresponding to the expiration of
    /// the url associated with this file.
    pub expiration_timestamp: Option<String>,
}

/// Build a remove action
pub struct RemoveBuilder {
    url: String,
    id: String,
    partition_values: HashMap<String, Option<String>>,
    size: u64,
    stats: Option<String>,
    version: Option<u64>,
    timestamp: Option<String>,
    expiration_timestamp: Option<String>,
}

impl RemoveBuilder {
    /// Initialize a new RemoveBuilder.
    pub fn new<S: Into<String>>(url: S, id: S, size: u64) -> Self {
        Self {
            url: url.into(),
            id: id.into(),
            partition_values: HashMap::new(),
            size,
            stats: None,
            version: None,
            timestamp: None,
            expiration_timestamp: None,
        }
    }

    /// Set the partition values for this file.
    pub fn partition_values(mut self, partition_values: HashMap<String, Option<String>>) -> Self {
        self.partition_values = partition_values;
        self
    }

    /// Add a partition value for this file.
    pub fn add_partition_value<S: Into<String>>(mut self, partition: S, value: Option<S>) -> Self {
        self.partition_values
            .insert(partition.into(), value.map(Into::into));
        self
    }

    /// Set file level statistics for this file.
    pub fn stats(mut self, stats: impl Into<String>) -> Self {
        self.stats = Some(stats.into());
        self
    }

    /// Expiration timestamp for the url associated with this file.
    pub fn expiration_timestamp(mut self, ts: impl Into<String>) -> Self {
        self.expiration_timestamp = Some(ts.into());
        self
    }

    /// Build a Remove from the provided configuration.
    pub fn build(self) -> Remove {
        Remove {
            url: self.url,
            id: self.id,
            partition_values: self.partition_values,
            size: self.size,
            stats: self.stats,
            version: self.version.unwrap_or(0),
            timestamp: self.timestamp.unwrap_or("0".to_string()),
            expiration_timestamp: self.expiration_timestamp,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::FileBuilder;

    #[test]
    fn serialize_file() {
        let file = FileBuilder::new(
            "https://<s3-bucket-name>.s3.us-west-2.amazonaws.com/tbl/f1.snappy.parquet",
            "591723a8-6a27-4240-a90e-57426f4736d2",
        )
        .add_partition_value("date", Some("2021-04-28"))
        .stats("{\"numRecords\":1,\"minValues\":{\"eventTime\":\"2021-04-28T23:33:48.719Z\"},\"maxValues\":{\"eventTime\":\"2021-04-28T23:33:48.719Z\"},\"nullCount\":{\"eventTime\":0}}")
        .expiration_timestamp("1652140800000")
        .build();

        insta::assert_json_snapshot!(file)
    }
}
