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
    pub min_reader_version: u32,
}

/// Representation of the table metadata.
///
/// The metadata of a table contains all the information required to correctly
/// interpret the data files of the table.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    /// Unique table identifier
    pub id: String,
    /// Table name provided by the user
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Table description provided by the user
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Specification of the table format
    pub format: FileFormat,
    /// Schema of the table, serialized as a string. This string can be
    /// deserialized into a Schema type.
    pub schema_string: String,
    /// An array of column names that are used to partition the table.
    pub partition_columns: Vec<String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    /// A map containing configuration options for the table.
    pub configuration: HashMap<String, Option<String>>,
    /// The version of the table this metadata corresponds to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// The size of the table in bytes.
    pub size: Option<u64>,
    /// The number of files in the table.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_files: Option<u64>,
}

/// Representation of the table format.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FileFormat {
    /// The format of the data files backing the shared table.
    pub provider: String,
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
    pub partition_values: HashMap<String, String>,
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
    pub partition_values: HashMap<String, String>,
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
    pub partition_values: HashMap<String, String>,
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
    pub partition_values: HashMap<String, String>,
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
