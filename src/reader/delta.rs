use async_trait::async_trait;
use deltalake::DeltaTableError;

use crate::reader::UnsignedDataFile;

use super::{
    TableMetadata, TableReader, TableReaderError, TableVersionNumber, UnsignedTableData, Version,
    VersionRange,
};

/// TableReader implementation for the Delta Lake format.
#[derive(Debug, Clone, PartialEq)]
pub struct DeltaTableReader;

impl DeltaTableReader {
    /// Create a new instance of the Delta Lake TableReader.
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for DeltaTableReader {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TableReader for DeltaTableReader {
    async fn get_table_version_number(
        &self,
        storage_path: &str,
        version: Version,
    ) -> Result<TableVersionNumber, TableReaderError> {
        match version {
            Version::Latest => {
                let delta_table = deltalake::open_table(storage_path).await?;
                Ok(delta_table.version() as u64)
            }
            Version::Number(version) => {
                let delta_table =
                    deltalake::open_table_with_version(storage_path, version as i64).await?;
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

        let protocol = delta_table.protocol()?.clone();
        let metadata = delta_table.metadata()?.clone();

        Ok(TableMetadata {
            version: delta_table.version() as u64,
            protocol,
            metadata,
        })
    }

    async fn get_table_data(
        &self,
        storage_path: &str,
        version: Version,
        _limit: Option<u64>,
        _predicates: Option<String>,
    ) -> Result<UnsignedTableData, TableReaderError> {
        tracing::info!(
            "get_table_data: storage_path={}, version={:?}",
            storage_path,
            version
        );
        let mut delta_table = match version {
            Version::Latest => deltalake::open_table(storage_path).await?,
            Version::Number(version) => {
                deltalake::open_table_with_version(storage_path, version as i64).await?
            }
            Version::Timestamp(ts) => {
                deltalake::open_table_with_ds(storage_path, ts.to_rfc3339()).await?
            }
        };

        let protocol = delta_table.protocol()?.clone();
        let metadata = delta_table.metadata()?.clone();

        let mut table_files = vec![];
        for file in delta_table.state.as_ref().unwrap().file_actions()? {
            table_files.push(UnsignedDataFile::File(file));
        }

        Ok(UnsignedTableData {
            version: delta_table.version() as u64,
            protocol,
            metadata,
            data: table_files,
        })
    }

    async fn get_table_changes(
        &self,
        _storage_path: &str,
        _range: VersionRange,
    ) -> Result<UnsignedTableData, TableReaderError> {
        Err(TableReaderError::Other)
    }
}

impl From<DeltaTableError> for TableReaderError {
    fn from(_value: DeltaTableError) -> Self {
        // TODO: meaningful error handling
        TableReaderError::Other
    }
}
