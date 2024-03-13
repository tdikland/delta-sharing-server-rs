use std::collections::HashMap;

use axum::response::{IntoResponse, Response};
use bytes::{BufMut, BytesMut};
use http::{header, StatusCode};

pub struct DeltaResponse {
    version: u64,
    protocol: Protocol,
    metadata: Metadata,
}

impl IntoResponse for DeltaResponse {
    fn into_response(self) -> Response {
        let mut buf = BytesMut::new().writer();

        // serde_json::to_writer(&mut buf, &self.protocol).unwrap();
        // buf.write_all(b"\n").unwrap();

        // serde_json::to_writer(&mut buf, &self.metadata).unwrap();
        // buf.write_all(b"\n").unwrap();

        // for line in self.lines {
        //     serde_json::to_writer(&mut buf, &line).unwrap();
        //     buf.write_all(b"\n").unwrap();
        // }

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

struct DeltaProtocol {
    min_reader_version: u32,
    min_writer_version: u32,
    reader_features: Vec<String>,
    writer_features: Vec<String>,
}

struct Protocol {
    delta_protocol: DeltaProtocol,
}

struct Format {
    provider: String,
    options: Option<HashMap<String, String>>,
}

struct DeltaMetadata {
    id: String,
    name: Option<String>,
    description: Option<String>,
    format: Format,
    schema_string: String,
    partition_columns: Vec<String>,
    created_time: Option<u64>,
    configuration: HashMap<String, String>,
}

struct Metadata {
    delta_metadata: DeltaMetadata,
    version: Option<u64>,
    size: Option<u64>,
    num_files: Option<u64>,
}

struct Action;

struct File {
    id: String,
    deletion_vector_file_id: Option<String>,
    version: Option<u64>,
    timestamp: Option<u64>,
    expiration_timestamp: Option<u64>,
    delta_single_action: Action,
}
