use axum::response::{IntoResponse, Response};
use bytes::{BufMut, BytesMut};
use deltalake::kernel::{Add, AddCDCFile, Metadata, Protocol, Remove};
use http::{header, StatusCode};
use serde::Serialize;
use std::{collections::HashMap, io::Write};

use crate::{
    reader::TableMetadata,
    signer::{SignedDataFile, SignedTableData},
};

pub struct ParquetResponse {
    version: u64,
    protocol: ParquetResponseLine,
    metadata: ParquetResponseLine,
    lines: Vec<ParquetResponseLine>,
}

impl IntoResponse for ParquetResponse {
    fn into_response(self) -> Response {
        let mut buf = BytesMut::new().writer();

        serde_json::to_writer(&mut buf, &self.protocol).unwrap();
        buf.write_all(b"\n").unwrap();

        serde_json::to_writer(&mut buf, &self.metadata).unwrap();
        buf.write_all(b"\n").unwrap();

        for line in self.lines {
            serde_json::to_writer(&mut buf, &line).unwrap();
            buf.write_all(b"\n").unwrap();
        }

        let version = self.version.to_string();
        let headers = [
            (
                header::CONTENT_TYPE.as_str(),
                "application/x-ndjson; charset=utf-8",
            ),
            ("Delta-Table-Version", version.as_ref()),
        ];

        (StatusCode::OK, headers, buf.into_inner()).into_response()
    }
}

impl From<TableMetadata> for ParquetResponse {
    fn from(table_metadata: TableMetadata) -> Self {
        Self {
            version: table_metadata.version,
            protocol: ParquetResponseLine::Protocol(table_metadata.protocol.into()),
            metadata: ParquetResponseLine::Metadata(table_metadata.metadata.into()),
            lines: vec![],
        }
    }
}

impl From<SignedTableData> for ParquetResponse {
    fn from(signed_table_data: SignedTableData) -> Self {
        // TODO: transform lines
        let lines = signed_table_data
            .data
            .into_iter()
            .map(Into::into)
            .collect::<Vec<_>>();

        Self {
            version: signed_table_data.version,
            protocol: ParquetResponseLine::Protocol(signed_table_data.protocol.into()),
            metadata: ParquetResponseLine::Metadata(signed_table_data.metadata.into()),
            lines,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
enum ParquetResponseLine {
    Protocol(ProtocolParquetLine),
    Metadata(MetadataParquetLine),
    File(FileParquetLine),
    Add(AddParquetLine),
    Cdf(CdfParquetLine),
    Remove(RemoveParquetLine),
}

impl From<SignedDataFile> for ParquetResponseLine {
    fn from(signed_data_file: SignedDataFile) -> Self {
        match signed_data_file {
            SignedDataFile::File(file) => ParquetResponseLine::File(file.into()),
            SignedDataFile::Add(add) => ParquetResponseLine::Add(add.into()),
            SignedDataFile::Cdf(cdf) => ParquetResponseLine::Cdf(cdf.into()),
            SignedDataFile::Remove(remove) => ParquetResponseLine::Remove(remove.into()),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct ProtocolParquetLine {
    min_reader_version: u32,
}

impl From<Protocol> for ProtocolParquetLine {
    fn from(protocol: Protocol) -> Self {
        Self {
            min_reader_version: protocol.min_reader_version as u32,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct Format {
    provider: String,
}

#[derive(Debug, Clone, Serialize)]
struct MetadataParquetLine {
    id: String,
    name: Option<String>,
    description: Option<String>,
    format: Format,
    schema_string: String,
    partition_columns: Vec<String>,
    configuration: Option<HashMap<String, String>>,
    version: Option<u64>,
    size: Option<u64>,
    num_files: Option<u64>,
}

impl From<Metadata> for MetadataParquetLine {
    fn from(metadata: Metadata) -> Self {
        Self {
            id: metadata.id,
            name: metadata.name,
            description: metadata.description,
            format: Format {
                provider: metadata.format.provider,
            },
            schema_string: metadata.schema_string,
            partition_columns: metadata.partition_columns,
            // TODO: can we derive these properties?
            configuration: None,
            version: None,
            size: None,
            num_files: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct FileParquetLine {
    url: String,
    id: String,
    parition_values: Vec<String>,
    size: u64,
    stats: Option<String>,
    version: Option<u64>,
    timestamp: Option<u64>,
    expiration_timestamp: Option<u64>,
}

impl From<Add> for FileParquetLine {
    fn from(add: Add) -> Self {
        // TODO: Figure out this conversion
        Self {
            url: add.path,
            id: String::from("TODO"),
            parition_values: vec![],
            size: add.size as u64,
            stats: add.stats,
            version: None,
            timestamp: None,
            expiration_timestamp: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct AddParquetLine {
    url: String,
    id: String,
    partition_values: Vec<String>,
    size: u64,
    timestamp: u64,
    version: u32,
    stats: Option<String>,
    expiration_timestamp: Option<u64>,
}

impl From<Add> for AddParquetLine {
    fn from(add: Add) -> Self {
        todo!()
    }
}

#[derive(Debug, Clone, Serialize)]
struct CdfParquetLine {
    url: String,
    id: String,
    partition_values: Vec<String>,
    size: u64,
    timestamp: u64,
    version: u32,
    expiration_timestamp: Option<u64>,
}

impl From<AddCDCFile> for CdfParquetLine {
    fn from(add_cdc_file: AddCDCFile) -> Self {
        todo!()
    }
}

#[derive(Debug, Clone, Serialize)]
struct RemoveParquetLine {
    url: String,
    id: String,
    partition_values: Vec<String>,
    size: u64,
    timestamp: u64,
    version: u32,
    expiration_timestamp: Option<u64>,
}

impl From<Remove> for RemoveParquetLine {
    fn from(remove: Remove) -> Self {
        todo!()
    }
}
