use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    protocol::{
        Metadata, Protocol, TableMetadata, TableVersionNumber, UnsignedTableData, Version,
        VersionRange,
    },
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
    ) -> Result<TableVersionNumber, TableReaderError>;

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
    ) -> Result<UnsignedTableData, TableReaderError>;

    async fn get_table_changes(
        &self,
        storage_path: &str,
        range: VersionRange,
    ) -> Result<UnsignedTableData, TableReaderError>;
}

#[derive(Debug, Clone)]
pub enum TableReaderError {
    Other,
}

// #[derive(Debug, Clone, Copy, PartialEq)]
// pub enum Version {
//     Latest,
//     Timestamp(DateTime<Utc>),
// }

// #[derive(Debug, Clone, Copy)]
// pub enum VersionRange {
//     Version {
//         start: u64,
//         end: u64,
//     },
//     Timestamp {
//         start: DateTime<Utc>,
//         end: DateTime<Utc>,
//     },
// }

// pub type TableVersionNumber = u64;

// #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
// pub struct TableMetadata {
//     pub version: u64,
//     pub protocol: Protocol,
//     pub metadata: Metadata,
// }

// #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
// pub struct UnsignedTableData {
//     pub version: u64,
//     pub protocol: Protocol,
//     pub metadata: Metadata,
//     pub data: Vec<DataFile>,
// }

// impl UnsignedTableData {
//     pub async fn sign(mut self, signer: &dyn UrlSigner) -> SignedTableData {
//         self.data.iter_mut().map(|file| file.sign(signer));

//         SignedTableData {
//             version: self.version,
//             protocol: self.protocol,
//             metadata: self.metadata,
//             data: self.data,
//         }
//     }
// }

// #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
// pub struct SignedTableData {
//     pub version: u64,
//     pub protocol: Protocol,
//     pub metadata: Metadata,
//     pub data: Vec<DataFile>,
// }
