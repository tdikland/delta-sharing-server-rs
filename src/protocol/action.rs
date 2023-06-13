//! Basic types for describing table data and metadata

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Representation of the table protocol.
///
/// Protocol versioning will allow servers to exclude older clients that are
/// missing features required to correctly interpret their response if the
/// Delta Sharing Protocol evolves in the future. The protocol version will be
/// increased whenever non-forward-compatible changes are made to the
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
    /// An HTTPS url that a client can use to directly read the data file.
    pub url: String,
    /// A unique identifier for the data file in the table.
    pub id: String,
    /// A map from partition column to value for this file in the table.
    pub partition_values: HashMap<String, Option<String>>,
    /// The size of this file in bytes.
    pub size: u64,
    /// Summary statistics about the data in this file.
    pub stats: Option<String>,
    /// The table version associated with this file.
    pub version: Option<u64>,
    /// The unix timestamp in milliseconds corresponding to the table version
    /// associated with this file.
    pub timestamp: Option<String>,
    /// The unix timestamp in milliseconds corresponding to the expiration of
    /// the url associated with this file.
    pub expiration_timestamp: Option<String>,
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
