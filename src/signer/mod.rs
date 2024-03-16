//! Traits and types for creating pre-signed urls.

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::reader::UnsignedDataFile;
use deltalake::kernel::{Add, AddCDCFile, Metadata, Protocol, Remove};

pub mod registry;

mod adls;
mod gcs;
pub mod noop;
pub mod s3;

/// Trait implemented by object store clients to derive a pre-signed url from
/// a object store path/prefix.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait UrlSigner: Send + Sync {
    /// Create a presigned url from a object store path.
    async fn sign_url(&self, path: &str) -> SignedUrl;

    /// Create a presigned url for a object store path within a data file.
    async fn sign_data_file(&self, data_file: crate::reader::UnsignedDataFile) -> SignedDataFile {
        match data_file {
            UnsignedDataFile::File(mut file) => {
                file.path = self.sign_url(&file.path).await.url;
                SignedDataFile::File(file)
            }
            UnsignedDataFile::Add(mut add) => {
                add.path = self.sign_url(&add.path).await.url;
                SignedDataFile::Add(add)
            }
            UnsignedDataFile::Cdf(mut cdf) => {
                cdf.path = self.sign_url(&cdf.path).await.url;
                SignedDataFile::Cdf(cdf)
            }
            UnsignedDataFile::Remove(mut remove) => {
                remove.path = self.sign_url(&remove.path).await.url;
                SignedDataFile::Remove(remove)
            }
        }
    }

    /// Create presigned urls for all data files in a table version.
    async fn sign_table_data(
        &self,
        table_data: crate::reader::UnsignedTableData,
    ) -> SignedTableData {
        let mut signed_data_files = vec![];
        for data_file in table_data.data {
            signed_data_files.push(self.sign_data_file(data_file).await);
        }
        SignedTableData {
            version: table_data.version,
            protocol: table_data.protocol,
            metadata: table_data.metadata,
            data: signed_data_files,
        }
    }
}

/// A presigned url with a validity period.
pub struct SignedUrl {
    url: String,
    valid_from: DateTime<Utc>,
    valid_duration: Duration,
}

impl SignedUrl {
    fn new(url: String, valid_from: DateTime<Utc>, valid_duration: Duration) -> Self {
        Self {
            url,
            valid_from,
            valid_duration,
        }
    }

    /// Get the presigned url.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Get the time the presigned url expires.
    pub fn expires_at(&self) -> DateTime<Utc> {
        self.valid_from + self.valid_duration
    }
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

/// A representation of data or mutation in a table reachable with a presigned
/// url.
///
/// A table is represented as a set of files that together are the full table.
/// Every data file has a presigned url that can be used to directly access the
/// data file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[serde(untagged)]
pub enum SignedDataFile {
    /// A file containing data part of the table.
    File(Add),
    /// A file containing data that was added to the table in this version.
    Add(Add),
    /// A file containing data that was changed in this version of the table.
    Cdf(AddCDCFile),
    /// A file containing data that was removed since the last table version.
    Remove(Remove),
}
