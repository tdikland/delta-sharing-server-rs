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
            protocol: TableProtocol {
                inner: core_to_kernel_p(protocol),
            },
            metadata: TableMetadata {
                inner: core_to_kernel_m(metadata),
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
            table_files.push(
                TableFile {
                    inner: core_to_kernel_add(file),
                    size: None,
                }
                .into(),
            );
        }

        Ok(TableData {
            version: delta_table.version() as u64,
            protocol: TableProtocol {
                inner: core_to_kernel_p(protocol),
            },
            metadata: TableMetadata {
                inner: core_to_kernel_m(metadata),
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

fn core_to_kernel_p(p: deltalake::kernel::Protocol) -> delta_kernel::actions::Protocol {
    delta_kernel::actions::Protocol {
        min_reader_version: p.min_reader_version,
        min_writer_version: p.min_writer_version,
        reader_features: p
            .reader_features
            .map(|s| s.into_iter().map(|f| f.to_string()).collect()),
        writer_features: p
            .writer_features
            .map(|s| s.into_iter().map(|f| f.to_string()).collect()),
    }
}

fn core_to_kernel_fmt(fmt: deltalake::kernel::Format) -> delta_kernel::actions::Format {
    delta_kernel::actions::Format {
        provider: fmt.provider,
        options: fmt
            .options
            .into_iter()
            .map(|(k, v)| (k, v.unwrap_or_default()))
            .collect(),
    }
}

fn core_to_kernel_m(m: deltalake::kernel::Metadata) -> delta_kernel::actions::Metadata {
    delta_kernel::actions::Metadata {
        id: m.id,
        name: m.name,
        description: m.description,
        format: core_to_kernel_fmt(m.format),
        schema_string: m.schema_string,
        partition_columns: m.partition_columns,
        configuration: m.configuration,
        created_time: m.created_time,
    }
}

fn core_to_kernel_dv(
    dv: deltalake::kernel::DeletionVectorDescriptor,
) -> delta_kernel::actions::DeletionVectorDescriptor {
    delta_kernel::actions::DeletionVectorDescriptor {
        storage_type: dv.storage_type.to_string(),
        path_or_inline_dv: dv.path_or_inline_dv,
        offset: dv.offset,
        size_in_bytes: dv.size_in_bytes,
        cardinality: dv.cardinality,
    }
}

fn core_to_kernel_add(add: deltalake::kernel::Add) -> delta_kernel::actions::Add {
    delta_kernel::actions::Add {
        path: add.path,
        size: add.size,
        partition_values: add.partition_values,
        modification_time: add.modification_time,
        data_change: add.data_change,
        stats: add.stats,
        tags: add.tags.unwrap_or_default(),
        deletion_vector: (add.deletion_vector).map(core_to_kernel_dv),
        base_row_id: add.base_row_id,
        default_row_commit_version: add.default_row_commit_version,
        clustering_provider: add.clustering_provider,
    }
}
