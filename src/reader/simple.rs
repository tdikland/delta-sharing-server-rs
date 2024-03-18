use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use delta_kernel::{
    actions::Add, scan::ScanBuilder, simple_client::SimpleClient, snapshot::Snapshot, Table,
};

use url::Url;

use super::{
    TableData, TableFile, TableMeta, TableMetadata, TableProtocol, TableReader, TableReaderError,
    TableVersionNumber, Version, VersionRange,
};

pub struct DeltaKernelReader {
    engine: SimpleClient,
}

impl DeltaKernelReader {
    pub fn new() -> DeltaKernelReader {
        DeltaKernelReader {
            engine: SimpleClient::new(),
        }
    }

    pub fn snapshot(
        &self,
        storage_path: &str,
        version: Version,
    ) -> Result<Arc<Snapshot>, delta_kernel::Error> {
        let delta_table_location: Url = storage_path.parse().expect("valid URL");
        let delta_table = Table::new(delta_table_location);

        let snapshot = match version {
            Version::Latest => delta_table.snapshot(&self.engine, None)?,
            Version::Number(version) => delta_table.snapshot(&self.engine, Some(version as u64))?,
            Version::Timestamp(_) => todo!(),
        };

        Ok(snapshot)
    }

    pub fn latest_snapshot(
        &self,
        storage_path: &str,
    ) -> Result<Arc<Snapshot>, delta_kernel::Error> {
        let delta_table_location: Url = storage_path.parse().expect("valid URL");
        let delta_table = Table::new(delta_table_location);

        let snapshot = delta_table.snapshot(&self.engine, None)?;

        Ok(snapshot)
    }
}

impl Default for DeltaKernelReader {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TableReader for DeltaKernelReader {
    async fn get_table_version_number(
        &self,
        storage_path: &str,
        version: Version,
    ) -> Result<TableVersionNumber, TableReaderError> {
        self.latest_snapshot(storage_path)
            .map(|s| TableVersionNumber(s.version() as u64))
            .map_err(|e| TableReaderError::Other)
    }

    async fn get_table_meta(&self, storage_path: &str) -> Result<TableMeta, TableReaderError> {
        let snap = self
            .latest_snapshot(storage_path)
            .map_err(|_| TableReaderError::Other)?;
        let meta = snap.metadata();
        let protocol = snap.protocol();

        Ok(TableMeta {
            version: snap.version() as u64,
            protocol: TableProtocol {
                inner: protocol.clone(),
            },
            metadata: TableMetadata {
                inner: meta.clone(),
                num_files: None,
                size: None,
            },
        })
    }

    async fn get_table_data(
        &self,
        storage_path: &str,
        version: Version,
        limit: Option<u64>,
        predicates: Option<String>,
        opt: Option<HashMap<String, String>>,
    ) -> Result<TableData, TableReaderError> {
        let snap = self
            .snapshot(storage_path, version)
            .map_err(|_| TableReaderError::Other)?;
        let meta = snap.metadata();
        let protocol = snap.protocol();

        let scan = ScanBuilder::new(snap.clone()).build();
        let files: Vec<Add> = scan
            .files(&self.engine)
            .map_err(|e| TableReaderError::Other)?
            .collect::<Result<_, _>>()
            .map_err(|e| TableReaderError::Other)?;

        Ok(TableData {
            version: snap.version() as u64,
            protocol: TableProtocol {
                inner: protocol.clone(),
            },
            metadata: TableMetadata {
                inner: meta.clone(),
                num_files: None,
                size: None,
            },
            data: files
                .into_iter()
                .map(|f| TableFile {
                    inner: f,
                    size: None,
                })
                .collect(),
        })
    }

    async fn get_table_changes(
        &self,
        storage_path: &str,
        range: VersionRange,
        opt: Option<HashMap<String, String>>,
    ) -> Result<TableData, TableReaderError> {
        unimplemented!()
    }
}
