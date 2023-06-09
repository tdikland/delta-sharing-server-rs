use async_trait::async_trait;

use crate::protocol::table::{
    TableMetadata, TableVersionNumber, UnsignedTableData, Version, VersionRange,
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
