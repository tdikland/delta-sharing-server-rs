use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::action::{Add, Cdf, File, Metadata, Protocol, Remove};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Version {
    Latest,
    Timestamp(DateTime<Utc>),
}

#[derive(Debug, Clone, Copy)]
pub enum VersionRange {
    Version {
        start: u64,
        end: u64,
    },
    Timestamp {
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    },
}

pub type TableVersionNumber = u64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableMetadata {
    pub version: u64,
    pub protocol: Protocol,
    pub metadata: Metadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UnsignedTableData {
    pub version: u64,
    pub protocol: Protocol,
    pub metadata: Metadata,
    pub data: Vec<UnsignedDataFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SignedTableData {
    pub version: u64,
    pub protocol: Protocol,
    pub metadata: Metadata,
    pub data: Vec<SignedDataFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum UnsignedDataFile {
    File(File),
    Add(Add),
    Remove(Remove),
    Cdf(Cdf),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum SignedDataFile {
    File(File),
    Add(Add),
    Remove(Remove),
    Cdf(Cdf),
}
