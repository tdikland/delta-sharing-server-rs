use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    protocol::table::{ChangeFile, DataFile, Metadata, Protocol, SignedChangeFile, SignedDataFile},
    signer::UrlSigner,
};

pub mod delta;

#[mockall::automock]
#[async_trait]
pub trait TableReader: Send + Sync {
    async fn get_table_version(
        &self,
        storage_path: &str,
        version: Version,
    ) -> Result<TableVersion, TableReaderError>;

    async fn get_table_metadata(
        &self,
        storage_path: &str,
    ) -> Result<TableMetadata, TableReaderError>;

    async fn get_table_data(
        &self,
        storage_path: &str,
        version: u64,
        limit: Option<u64>,
        predicates: Option<String>,
    ) -> Result<TableData, TableReaderError>;

    async fn get_table_changes(
        &self,
        storage_path: &str,
        range: VersionRange,
    ) -> Result<TableChanges, TableReaderError>;
}

#[derive(Debug, Clone)]
pub enum TableReaderError {
    Other,
}

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

pub type TableVersion = u64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableMetadata {
    pub version: u64,
    pub protocol: Protocol,
    pub metadata: Metadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableData {
    pub version: u64,
    pub protocol: Protocol,
    pub metadata: Metadata,
    pub data: Vec<DataFile>,
}

impl TableData {
    pub async fn sign(self, signer: &dyn UrlSigner) -> SignedTableData {
        let mut signed_data = vec![];
        for file in self.data {
            let signed_file = file.sign(signer).await;
            signed_data.push(signed_file);
        }

        SignedTableData {
            version: self.version,
            protocol: self.protocol,
            metadata: self.metadata,
            data: signed_data,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SignedTableData {
    pub version: u64,
    pub protocol: Protocol,
    pub metadata: Metadata,
    pub data: Vec<SignedDataFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableChanges {
    pub version: u64,
    pub protocol: Protocol,
    pub metadata: Metadata,
    pub changes: Vec<ChangeFile>,
}

impl TableChanges {
    pub async fn sign(self, signer: &dyn UrlSigner) -> SignedTableChanges {
        let mut signed_changes = vec![];
        for file in self.changes {
            let signed_file = file.sign(signer).await;
            signed_changes.push(signed_file);
        }

        SignedTableChanges {
            version: self.version,
            protocol: self.protocol,
            metadata: self.metadata,
            changes: signed_changes,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SignedTableChanges {
    pub version: u64,
    pub protocol: Protocol,
    pub metadata: Metadata,
    pub changes: Vec<SignedChangeFile>,
}
