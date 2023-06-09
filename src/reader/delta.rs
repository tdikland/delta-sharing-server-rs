use std::collections::HashMap;

use async_trait::async_trait;
use deltalake::DeltaTableError;

use crate::protocol::action::{Add, File, FileFormat, Metadata, Protocol};
use crate::protocol::table::{
    TableMetadata, TableVersionNumber, UnsignedDataFile, UnsignedTableData, Version, VersionRange,
};

use super::{TableReader, TableReaderError};

#[derive(Debug, Clone, PartialEq)]
pub struct DeltaTableReader;

impl DeltaTableReader {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl TableReader for DeltaTableReader {
    async fn get_table_version(
        &self,
        storage_path: &str,
        version: Version,
    ) -> Result<TableVersionNumber, TableReaderError> {
        match version {
            Version::Latest => {
                let delta_table = deltalake::open_table(storage_path).await?;
                Ok(delta_table.version() as u64)
            }
            Version::Timestamp(ts) => {
                let delta_table =
                    deltalake::open_table_with_ds(storage_path, ts.to_rfc3339()).await?;
                Ok(delta_table.version() as u64)
            }
        }
    }

    async fn get_table_metadata(
        &self,
        storage_path: &str,
    ) -> Result<TableMetadata, TableReaderError> {
        let delta_table = deltalake::open_table(storage_path).await?;

        let min_reader_version = delta_table.get_min_reader_version() as u32;
        let table_protocol = Protocol { min_reader_version };

        let metadata = delta_table.get_metadata()?;
        let table_metadata = Metadata {
            id: metadata.id.clone(),
            name: metadata.name.clone(),
            description: None,
            format: FileFormat {
                provider: String::from("parquet"),
            },
            schema_string: String::from(""),
            partition_columns: metadata.partition_columns.clone(),
            configuration: metadata.configuration.clone(),
            version: None,
            size: None,
            num_files: None,
        };

        Ok(TableMetadata {
            version: delta_table.version() as u64,
            protocol: table_protocol,
            metadata: table_metadata,
        })
    }

    async fn get_table_data(
        &self,
        storage_path: &str,
        _version: u64,
        _limit: Option<u64>,
        _predicates: Option<String>,
    ) -> Result<UnsignedTableData, TableReaderError> {
        let delta_table = deltalake::open_table(storage_path).await?;

        let min_reader_version = delta_table.get_min_reader_version() as u32;
        let table_protocol = Protocol { min_reader_version };

        let metadata = delta_table.get_metadata()?;
        let table_metadata = Metadata {
            id: metadata.id.clone(),
            name: metadata.name.clone(),
            description: None,
            format: FileFormat {
                provider: String::from("parquet"),
            },
            schema_string: String::from(""),
            partition_columns: metadata.partition_columns.clone(),
            configuration: metadata.configuration.clone(),
            version: None,
            size: None,
            num_files: None,
        };

        let mut table_files = vec![];
        for file in delta_table.get_state().files() {
            let url = format!("{}/{}", storage_path, file.path);
            let data_file = UnsignedDataFile::Add(Add {
                url,
                id: "some_id".to_string(),
                partition_values: HashMap::new(),
                size: 6,
                stats: None,
                version: 0,
                timestamp: String::from("0"),
                expiration_timestamp: None,
            });
            table_files.push(data_file);
        }

        Ok(UnsignedTableData {
            version: delta_table.version() as u64,
            protocol: table_protocol,
            metadata: table_metadata,
            data: table_files,
        })
    }

    async fn get_table_changes(
        &self,
        storage_path: &str,
        _range: VersionRange,
    ) -> Result<UnsignedTableData, TableReaderError> {
        let delta_table = deltalake::open_table(storage_path).await?;

        let min_reader_version = delta_table.get_min_reader_version() as u32;
        let table_protocol = Protocol { min_reader_version };

        let metadata = delta_table.get_metadata()?;
        let table_metadata = Metadata {
            id: metadata.id.clone(),
            name: metadata.name.clone(),
            description: None,
            format: FileFormat {
                provider: String::from("parquet"),
            },
            schema_string: String::from(""),
            partition_columns: metadata.partition_columns.clone(),
            configuration: metadata.configuration.clone(),
            version: None,
            size: None,
            num_files: None,
        };

        let mut table_files = vec![];
        for file in delta_table.get_state().files() {
            let url = format!("{}/{}", storage_path, file.path);
            let data_file = UnsignedDataFile::File(File {
                url,
                id: "some_id".to_string(),
                partition_values: HashMap::new(),
                size: 6,
                stats: None,
                version: None,
                timestamp: None,
                expiration_timestamp: None,
            });
            table_files.push(data_file);
        }

        Ok(UnsignedTableData {
            version: delta_table.version() as u64,
            protocol: table_protocol,
            metadata: table_metadata,
            data: vec![],
        })
    }
}

impl From<DeltaTableError> for TableReaderError {
    fn from(value: DeltaTableError) -> Self {
        // TODO: meaningful error handling
        match value {
            _ => TableReaderError::Other,
        }
    }
}
