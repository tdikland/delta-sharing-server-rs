use axum::response::{IntoResponse, Response};
use bytes::{BufMut, BytesMut};
use http::{header, StatusCode};
use serde::Serialize;
use std::{collections::HashMap, io::Write};

use crate::protocol::table::TableMetadata;

struct ParquetResponse {
    version: u64,
    protocol: Protocol,
    metadata: Metadata,
    lines: Vec<ParquetResponseLine>,
}

#[derive(Debug, Clone, Serialize)]
enum ParquetResponseLine {
    Protocol(Protocol),
    Metadata(Metadata),
    Add(Add),
    Cdf(Cdf),
    Remove(Remove),
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

// impl From<TableMetadata> for ParquetResponse {
//     fn from(metadata: TableMetadata) -> Self {
//         Self {
//             version: metadata.version.unwrap_or(0),
//             protocol,
//             metadata,
//             lines: vec![],
//         }
//     }
// }

#[derive(Debug, Clone, Serialize)]
struct Protocol {
    min_reader_version: u32,
}

#[derive(Debug, Clone, Serialize)]
struct Format {
    provider: String,
}

#[derive(Debug, Clone, Serialize)]
struct Metadata {
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

#[derive(Debug, Clone, Serialize)]
struct File {
    url: String,
    id: String,
    parition_values: Vec<String>,
    size: u64,
    stats: Option<String>,
    version: Option<u64>,
    timestamp: Option<u64>,
    expiration_timestamp: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
struct Add {
    url: String,
    id: String,
    partition_values: Vec<String>,
    size: u64,
    timestamp: u64,
    version: u32,
    stats: Option<String>,
    expiration_timestamp: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
struct Cdf {
    url: String,
    id: String,
    partition_values: Vec<String>,
    size: u64,
    timestamp: u64,
    version: u32,
    expiration_timestamp: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
struct Remove {
    url: String,
    id: String,
    partition_values: Vec<String>,
    size: u64,
    timestamp: u64,
    version: u32,
    expiration_timestamp: Option<u64>,
}
