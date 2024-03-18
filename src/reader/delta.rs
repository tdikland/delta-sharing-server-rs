use std::collections::HashMap;

use async_trait::async_trait;
use deltalake::DeltaTableError;

use crate::reader::{TableFile, TableMetadata, TableProtocol};

use super::{
    TableData, TableMeta, TableReader, TableReaderError, TableVersionNumber, Version, VersionRange,
};

/// TableReader implementation for the Delta Lake format.
#[derive(Debug, Clone, PartialEq)]
pub struct DeltaTableReader;

impl DeltaTableReader {
    /// Create a new instance of the Delta Lake TableReader.
    pub fn new() -> Self {
        deltalake::aws::register_handlers(None);
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
        let table = match version {
            Version::Latest => deltalake::open_table(storage_path).await?,
            Version::Number(version) => {
                deltalake::open_table_with_version(storage_path, version as i64).await?
            }
            Version::Timestamp(ts) => {
                deltalake::open_table_with_ds(storage_path, ts.to_rfc3339()).await?
            }
        };
        Ok(TableVersionNumber(table.version() as u64))
    }

    async fn get_table_meta(&self, storage_path: &str) -> Result<TableMeta, TableReaderError> {
        tracing::info!(path = ?storage_path, "get table metadata");
        let delta_table = deltalake::open_table(storage_path).await?;

        let protocol = delta_table.protocol()?.clone();
        let metadata = delta_table.metadata()?.clone();

        Ok(TableMeta {
            version: delta_table.version() as u64,
            protocol: TableProtocol { inner: protocol },
            metadata: TableMetadata {
                inner: metadata,
                num_files: None,
                size: None,
            },
        })
    }

    async fn get_table_data(
        &self,
        storage_path: &str,
        version: Version,
        _limit: Option<u64>,
        _predicates: Option<String>,
        _opt: Option<HashMap<String, String>>,
    ) -> Result<TableData, TableReaderError> {
        tracing::info!(
            "get_table_data: storage_path={}, version={:?}",
            storage_path,
            version
        );
        let delta_table = match version {
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
        for mut file in delta_table.state.as_ref().unwrap().file_actions()? {
            file.path = format!("{}/{}", storage_path, file.path);
            table_files.push(
                TableFile {
                    data: file,
                    size: None,
                }
                .into(),
            );
        }

        Ok(TableData {
            version: delta_table.version() as u64,
            protocol: TableProtocol { protocol },
            metadata: TableMetadata {
                metadata,
                num_files: None,
                size: None,
            },
            data: table_files,
        })
    }

    async fn get_table_changes(
        &self,
        _storage_path: &str,
        _range: VersionRange,
        _opt: Option<HashMap<String, String>>,
    ) -> Result<TableData, TableReaderError> {
        Err(TableReaderError::Other)
    }
}

impl From<DeltaTableError> for TableReaderError {
    fn from(_value: DeltaTableError) -> Self {
        tracing::error!("DeltaTableError: {}\n{:?}", _value, _value);
        // TODO: meaningful error handling
        TableReaderError::Other
    }
}
