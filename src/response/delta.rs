use std::io::Write;

use axum::response::{IntoResponse, Response};
use bytes::{BufMut, BytesMut};
use http::{header, StatusCode};
use serde::Serialize;

use crate::{
    reader::TableMetadata,
    signer::{SignedDataFile, SignedTableData},
};

use deltalake::kernel::{
    Add as DeltaAdd, AddCDCFile as DeltaCdf, Metadata as DeltaMetadata, Protocol as DeltaProtocol,
    Remove as DeltaRemove,
};

pub struct DeltaResponse {
    version: u64,
    protocol: DeltaResponseLine,
    metadata: DeltaResponseLine,
    lines: Vec<DeltaResponseLine>,
}

impl IntoResponse for DeltaResponse {
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

impl From<TableMetadata> for DeltaResponse {
    fn from(table_metadata: TableMetadata) -> Self {
        Self {
            version: table_metadata.version,
            protocol: DeltaResponseLine::Protocol(table_metadata.protocol.into()),
            metadata: DeltaResponseLine::Metadata(table_metadata.metadata.into()),
            lines: vec![],
        }
    }
}

impl From<SignedTableData> for DeltaResponse {
    fn from(signed_table_data: SignedTableData) -> Self {
        let lines = signed_table_data.data.into_iter().map(Into::into).collect();

        Self {
            version: signed_table_data.version,
            protocol: DeltaResponseLine::Protocol(signed_table_data.protocol.into()),
            metadata: DeltaResponseLine::Metadata(signed_table_data.metadata.into()),
            lines,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
enum DeltaResponseLine {
    Protocol(ProtocolResponseLine),
    Metadata(MetadataResponseLine),
    File(FileResponseLine),
}

impl From<SignedDataFile> for DeltaResponseLine {
    fn from(signed_data_file: SignedDataFile) -> Self {
        match signed_data_file {
            SignedDataFile::File(file) => DeltaResponseLine::File(file.into()),
            SignedDataFile::Add(add) => DeltaResponseLine::File(add.into()),
            SignedDataFile::Cdf(cdf) => DeltaResponseLine::File(cdf.into()),
            SignedDataFile::Remove(remove) => DeltaResponseLine::File(remove.into()),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct ProtocolResponseLine {
    delta_protocol: DeltaProtocol,
}

impl From<DeltaProtocol> for ProtocolResponseLine {
    fn from(delta_protocol: DeltaProtocol) -> Self {
        Self { delta_protocol }
    }
}

#[derive(Debug, Clone, Serialize)]
struct MetadataResponseLine {
    delta_metadata: DeltaMetadata,
    version: Option<u64>,
    size: Option<u64>,
    num_files: Option<u64>,
}

impl From<DeltaMetadata> for MetadataResponseLine {
    fn from(delta_metadata: DeltaMetadata) -> Self {
        Self {
            delta_metadata,
            version: None,
            size: None,
            num_files: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
enum SingleAction {
    Add(DeltaAdd),
    Cdf(DeltaCdf),
    Remove(DeltaRemove),
}

#[derive(Debug, Clone, Serialize)]
struct FileResponseLine {
    id: String,
    deletion_vector_file_id: Option<String>,
    version: Option<u64>,
    timestamp: Option<u64>,
    expiration_timestamp: Option<u64>,
    delta_single_action: SingleAction,
}

impl From<DeltaAdd> for FileResponseLine {
    fn from(add: DeltaAdd) -> Self {
        Self {
            id: String::from("TODO"),
            deletion_vector_file_id: None,
            version: None,
            timestamp: None,
            expiration_timestamp: None,
            delta_single_action: SingleAction::Add(add),
        }
    }
}

impl From<DeltaCdf> for FileResponseLine {
    fn from(add_cdc_file: DeltaCdf) -> Self {
        Self {
            id: String::from("TODO"),
            deletion_vector_file_id: None,
            version: None,
            timestamp: None,
            expiration_timestamp: None,
            delta_single_action: SingleAction::Cdf(add_cdc_file),
        }
    }
}

impl From<DeltaRemove> for FileResponseLine {
    fn from(remove: DeltaRemove) -> Self {
        Self {
            id: String::from("TODO"),
            deletion_vector_file_id: None,
            version: None,
            timestamp: None,
            expiration_timestamp: None,
            delta_single_action: SingleAction::Remove(remove),
        }
    }
}
