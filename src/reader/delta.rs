use async_trait::async_trait;
use deltalake::DeltaTableError;

use crate::protocol::action::{File, MetadataBuilder, Protocol};
use crate::protocol::table::{
    TableMetadata, TableVersionNumber, UnsignedDataFile, UnsignedTableData, Version, VersionRange,
};

use super::{TableReader, TableReaderError};

/// TableReader implementation for the Delta Lake format.
#[derive(Debug, Clone, PartialEq)]
pub struct DeltaTableReader;

impl DeltaTableReader {
    /// Create a new instance of the Delta Lake TableReader.
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
        let schema = serde_json::to_string(&delta_table.get_schema()?).unwrap();
        let configuration = metadata
            .configuration
            .clone()
            .into_iter()
            .map(|c| (c.0.clone(), c.1.unwrap_or_default()))
            .collect();
        let table_metadata = MetadataBuilder::new(metadata.id.clone(), schema)
            .partition_columns(metadata.partition_columns.clone())
            .configuration(configuration)
            .build();

        Ok(TableMetadata {
            version: delta_table.version() as u64,
            protocol: table_protocol,
            metadata: table_metadata,
        })
    }

    async fn get_table_data(
        &self,
        storage_path: &str,
        version: u64,
        _limit: Option<u64>,
        _predicates: Option<String>,
    ) -> Result<UnsignedTableData, TableReaderError> {
        let mut delta_table = deltalake::open_table(storage_path).await?;
        delta_table.load_version(version as i64).await?;

        let min_reader_version = delta_table.get_min_reader_version() as u32;
        let table_protocol = Protocol { min_reader_version };

        let metadata = delta_table.get_metadata()?;
        let schema = serde_json::to_string(&delta_table.get_schema()?).unwrap();
        let configuration = metadata
            .configuration
            .clone()
            .into_iter()
            .map(|c| (c.0.clone(), c.1.unwrap_or_default()))
            .collect();
        let table_metadata = MetadataBuilder::new(metadata.id.clone(), schema)
            .partition_columns(metadata.partition_columns.clone())
            .configuration(configuration)
            .build();

        let mut table_files = vec![];
        for file in delta_table.get_state().files() {
            let url = format!("{}/{}", storage_path, file.path);
            let data_file = UnsignedDataFile::File(File {
                url,
                id: "".to_string(),
                partition_values: file.partition_values.clone(),
                size: file.size as u64,
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
            data: table_files,
        })
    }

    async fn get_table_changes(
        &self,
        _storage_path: &str,
        _range: VersionRange,
    ) -> Result<UnsignedTableData, TableReaderError> {
        return Err(TableReaderError::Other);
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
