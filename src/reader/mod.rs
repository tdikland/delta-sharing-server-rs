//! Types and traits for reading table data in object storage.

use std::{collections::HashMap, error::Error, fmt::Display};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Serialize;

use delta_kernel::actions::{Add, Metadata, Protocol};

/// Table reader implementation for the Delta Lake format.
// pub mod delta;
pub mod simple;

/// Trait for reading a specific table format from cloud storage.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait TableReader: Send + Sync {
    /// Retrieve the table version number that corresponds to the version
    /// request.
    async fn get_table_version_number(
        &self,
        storage_path: &str,
        version: Version,
    ) -> Result<TableVersionNumber, TableReaderError>;

    /// Retrieve the table metadata corresponding to the latest table version.
    async fn get_table_meta(&self, storage_path: &str) -> Result<TableMeta, TableReaderError>;

    /// Retrieve the table data for a specific table version.
    ///
    /// The table data is represented by a collection of files which can be
    /// directly reached with a presigned HTTPS url. The limit and predicate
    /// argument can be used to restrict the returned data files on a best
    /// effort basis.
    async fn get_table_data(
        &self,
        storage_path: &str,
        version: Version,
        limit: Option<u64>,
        predicates: Option<String>,
        opt: Option<HashMap<String, String>>,
    ) -> Result<TableData, TableReaderError>;

    /// Retrieve the table change data for a specific range of table versions.
    ///
    /// The table changes are represented by a collection of files which can be
    /// directly reached with presigned HTTPS urls.
    async fn get_table_changes(
        &self,
        storage_path: &str,
        range: VersionRange,
        opt: Option<HashMap<String, String>>,
    ) -> Result<TableData, TableReaderError>;
}

/// Requested table version.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Version {
    /// Latest table version.
    Latest,
    /// Table version number.
    Number(u64),
    /// Earliest table version after the specified timestamp.
    Timestamp(DateTime<Utc>),
}

/// Requested range of table version.
#[derive(Debug, Clone, Copy)]
pub enum VersionRange {
    /// Range of table versions represented by start and end version number.
    Version {
        /// First timestamp that must be returned in the range.
        start: u64,
        /// Last timestamp that must be returned in the range.
        end: u64,
    },
    /// Range of table versions represented by start and end timestamp.
    Timestamp {
        /// First version must be the earliest after the start timestamp.
        start: DateTime<Utc>,
        /// Last version must be the earliest after the end timestamp.
        end: DateTime<Utc>,
    },
}

/// Table version number.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TableVersionNumber(u64);

impl TableVersionNumber {
    /// Create a new table version number.
    pub fn new(version: u64) -> Self {
        Self(version)
    }

    /// Get the table version number.
    pub fn version(&self) -> u64 {
        self.0
    }
}

/// Table metadata for a given table version.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TableMeta {
    version: u64,
    protocol: TableProtocol,
    metadata: TableMetadata,
}

impl TableMeta {
    /// Create a new table metadata.
    pub fn new(version: u64, protocol: TableProtocol, metadata: TableMetadata) -> Self {
        Self {
            version,
            protocol,
            metadata,
        }
    }

    /// Get the table version.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Get the table protocol.
    pub fn protocol(&self) -> &TableProtocol {
        &self.protocol
    }

    /// Get the table metadata.
    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    pub fn num_files(&self) -> Option<u64> {
        self.metadata.num_files
    }

    pub fn size(&self) -> Option<u64> {
        self.metadata.size
    }
}

/// Table metadata and data descriptors, not yet publicly accessible.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TableData {
    version: u64,
    protocol: TableProtocol,
    metadata: TableMetadata,
    data: Vec<TableFile>,
}

impl TableData {
    /// Create a new table data.
    pub fn new(
        version: u64,
        protocol: TableProtocol,
        metadata: TableMetadata,
        data: Vec<TableFile>,
    ) -> Self {
        Self {
            version,
            protocol,
            metadata,
            data,
        }
    }

    /// Get the table version.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Get the table protocol.
    pub fn protocol(&self) -> &TableProtocol {
        &self.protocol
    }

    /// Get the table metadata.
    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    /// Get the table data.
    pub fn data(&self) -> &[TableFile] {
        &self.data
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TableProtocol {
    inner: Protocol,
}

impl TableProtocol {
    pub fn new(inner: Protocol) -> Self {
        Self { inner }
    }

    pub fn into_inner(self) -> Protocol {
        self.inner
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TableMetadata {
    inner: Metadata,
    num_files: Option<u64>,
    size: Option<u64>,
}

impl TableMetadata {
    pub fn new(inner: Metadata, num_files: Option<u64>, size: Option<u64>) -> Self {
        Self {
            inner,
            num_files,
            size,
        }
    }

    pub fn num_files(&self) -> Option<u64> {
        self.num_files
    }

    pub fn size(&self) -> Option<u64> {
        self.size
    }

    pub fn into_inner(self) -> Metadata {
        self.inner
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TableFile {
    inner: Add,
    size: Option<u64>,
}

impl TableFile {
    pub fn new(inner: Add, size: Option<u64>) -> Self {
        Self { inner, size }
    }

    pub fn size(&self) -> Option<u64> {
        self.size
    }

    pub fn into_inner(self) -> Add {
        self.inner
    }
}

/// Error that occur during the reading of the table format.
#[derive(Debug, Clone)]
pub enum TableReaderError {
    /// An unexpected error occured.
    Other,
}

impl Display for TableReaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableReaderError::Other => {
                write!(f, "An unexpected error happened during table reading")
            }
        }
    }
}

impl Error for TableReaderError {}
