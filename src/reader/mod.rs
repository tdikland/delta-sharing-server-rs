//! Types and traits for reading table data in object storage.

use std::{error::Error, fmt::Display};

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::protocol::table::{TableMetadata, TableVersionNumber, UnsignedTableData};

/// Table reader implementation for the Delta Lake format.
pub mod delta;

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
    async fn get_table_metadata(
        &self,
        storage_path: &str,
    ) -> Result<TableMetadata, TableReaderError>;

    /// Retrieve the table data for a specific table version.
    ///
    /// The table data is represented by a collection of files which can be
    /// directly reached with a presigned HTTPS url. The limit and predicate
    /// argument can be used to restrict the returned data files on a best
    /// effort basis.
    async fn get_table_data(
        &self,
        storage_path: &str,
        version: u64,
        limit: Option<u64>,
        predicates: Option<String>,
    ) -> Result<UnsignedTableData, TableReaderError>;

    /// Retrieve the table change data for a specific range of table versions.
    ///
    /// The table changes are represented by a collection of files which can be
    /// directly reached with presigned HTTPS urls.
    async fn get_table_changes(
        &self,
        storage_path: &str,
        range: VersionRange,
    ) -> Result<UnsignedTableData, TableReaderError>;
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
