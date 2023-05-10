//! Types for table info request/response

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::action::{Add, Cdf, File, Metadata, Protocol, Remove};

/// Requested table version.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Version {
    /// Latest table version.
    Latest,
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
pub type TableVersionNumber = u64;

/// Table metadata for a given table version.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableMetadata {
    /// Table version.
    pub version: u64,
    /// Minimum required table reader protocol implementation.
    pub protocol: Protocol,
    /// Table metadata
    pub metadata: Metadata,
}

/// Table metadata and data descriptors, not yet publicly accessible.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UnsignedTableData {
    /// Table version.
    pub version: u64,
    /// Minimum required table reader protocol implementation.
    pub protocol: Protocol,
    /// Table metadata
    pub metadata: Metadata,
    /// Set of data file representing the table
    pub data: Vec<UnsignedDataFile>,
}

/// Table metadata and data descriptors with presigned urls.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SignedTableData {
    /// Table version.
    pub version: u64,
    /// Minimum required table reader protocol implementation.
    pub protocol: Protocol,
    /// Table metadata
    pub metadata: Metadata,
    /// Set of data file representing the table
    pub data: Vec<SignedDataFile>,
}

/// A representation of data or mutation in a table referenced using an object
/// store url.
///
/// A table is represented as a set of files that together are the full table.
/// Every data file has a reference to the underlying object store in its url
/// field.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum UnsignedDataFile {
    /// A file containing data part of the table.
    File(File),
    /// A file containing data that was added to the table in this version.
    Add(Add),
    /// A file containing data that was changed in this version of the table.
    Cdf(Cdf),
    /// A file containing data that was removed since the last table version.
    Remove(Remove),
}

/// A representation of data or mutation in a table reachable with a presigned
/// url.
///
/// A table is represented as a set of files that together are the full table.
/// Every data file has a presigned url that can be used to directly access the
/// data file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum SignedDataFile {
    /// A file containing data part of the table.
    File(File),
    /// A file containing data that was added to the table in this version.
    Add(Add),
    /// A file containing data that was changed in this version of the table.
    Cdf(Cdf),
    /// A file containing data that was removed since the last table version.
    Remove(Remove),
}

impl From<File> for UnsignedDataFile {
    fn from(v: File) -> Self {
        Self::File(v)
    }
}

impl From<Add> for UnsignedDataFile {
    fn from(v: Add) -> Self {
        Self::Add(v)
    }
}

impl From<Cdf> for UnsignedDataFile {
    fn from(v: Cdf) -> Self {
        Self::Cdf(v)
    }
}

impl From<Remove> for UnsignedDataFile {
    fn from(v: Remove) -> Self {
        Self::Remove(v)
    }
}
