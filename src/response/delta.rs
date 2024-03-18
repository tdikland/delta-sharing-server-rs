use std::{io::Write, sync::Arc};

use axum::response::{IntoResponse, Response};
use bytes::{BufMut, BytesMut};
use delta_kernel::actions::{Add, Metadata, Protocol};
use http::{header, StatusCode};
use serde::Serialize;
use url::Url;

use crate::reader::{TableData, TableMeta};
use crate::signer::UrlSigner;

pub struct DeltaResponse {
    version: u64,
    protocol: DeltaResponseLine,
    metadata: DeltaResponseLine,
    lines: Vec<DeltaResponseLine>,
}

impl DeltaResponse {
    pub async fn sign(&mut self, table_root: &str, signer: Arc<dyn UrlSigner>) {
        for line in self.lines.iter_mut() {
            line.sign(table_root, signer.clone()).await;
        }
    }
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

impl From<TableMeta> for DeltaResponse {
    fn from(meta: TableMeta) -> Self {
        let protocol = ProtocolResponseLine::from_protocol(meta.protocol().clone().into_inner());
        let metadata = MetadataResponseLine::from_metadata_with_opts(
            meta.metadata().clone().into_inner(),
            Some(meta.version()),
            None,
            None,
        );

        Self {
            version: meta.version(),
            protocol: protocol.into(),
            metadata: metadata.into(),
            lines: vec![],
        }
    }
}

impl From<TableData> for DeltaResponse {
    fn from(data: TableData) -> Self {
        let protocol = ProtocolResponseLine::from_protocol(data.protocol().clone().into_inner());
        let metadata = MetadataResponseLine::from_metadata_with_opts(
            data.metadata().clone().into_inner(),
            Some(data.version()),
            None,
            None,
        );

        let mut lines = vec![];
        for file in data.data().into_iter().cloned() {
            lines.push(
                FileResponseLine::from_add_with_opts(file.into_inner(), None, None, None).into(),
            );
        }
        Self {
            version: data.version(),
            protocol: protocol.into(),
            metadata: metadata.into(),
            lines,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
enum DeltaResponseLine {
    Protocol(ProtocolResponseLine),
    #[serde(rename = "metaData")]
    Metadata(MetadataResponseLine),
    File(FileResponseLine),
}

impl DeltaResponseLine {
    async fn sign(&mut self, table_root: &str, signer: Arc<dyn UrlSigner>) {
        match self {
            Self::File(file) => {
                file.sign(table_root, signer).await;
            }
            _ => {}
        }
    }
}

impl From<ProtocolResponseLine> for DeltaResponseLine {
    fn from(line: ProtocolResponseLine) -> Self {
        Self::Protocol(line)
    }
}

impl From<MetadataResponseLine> for DeltaResponseLine {
    fn from(line: MetadataResponseLine) -> Self {
        Self::Metadata(line)
    }
}

impl From<FileResponseLine> for DeltaResponseLine {
    fn from(line: FileResponseLine) -> Self {
        Self::File(line)
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProtocolResponseLine {
    delta_protocol: Protocol,
}

impl ProtocolResponseLine {
    fn from_protocol(protocol: Protocol) -> Self {
        Self {
            delta_protocol: protocol,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct MetadataResponseLine {
    delta_metadata: Metadata,
    version: Option<u64>,
    size: Option<u64>,
    num_files: Option<u64>,
}

impl MetadataResponseLine {
    fn from_metadata_with_opts(
        metadata: Metadata,
        version: Option<u64>,
        size: Option<u64>,
        num_files: Option<u64>,
    ) -> Self {
        Self {
            delta_metadata: metadata,
            version,
            size,
            num_files,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
enum SingleAction {
    Add(Add),
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct FileResponseLine {
    id: String,
    deletion_vector_file_id: Option<String>,
    version: Option<u64>,
    timestamp: Option<u64>,
    expiration_timestamp: Option<u64>,
    delta_single_action: SingleAction,
}

impl FileResponseLine {
    fn from_add_with_opts(
        add: Add,
        version: Option<u64>,
        timestamp: Option<u64>,
        expiration_timestamp: Option<u64>,
    ) -> Self {
        let file_id = format!("{:x}", md5::compute(add.path.as_bytes()));
        // TODO: derive deletion_vector_file_id
        Self {
            id: file_id,
            deletion_vector_file_id: None,
            version,
            timestamp,
            expiration_timestamp,
            delta_single_action: SingleAction::Add(add),
        }
    }

    async fn sign(&mut self, table_root: &str, signer: Arc<dyn UrlSigner>) {
        match &mut self.delta_single_action {
            SingleAction::Add(add) => {
                let file_url = format!("{}/{}", table_root, add.path);
                let signed_url = signer.sign_url(&file_url).await;
                add.path = signed_url.url().to_string();

                if let Some(dv) = &mut add.deletion_vector {
                    match dv.storage_type.as_str() {
                        "i" => (),
                        "u" => {
                            let parent = Url::parse(table_root).unwrap();
                            let deletion_vector_url = dv.absolute_path(&parent).unwrap().unwrap();
                            let signed_deletion_vector_url =
                                signer.sign_url(deletion_vector_url.as_str()).await;

                            dv.storage_type = "p".to_string();
                            dv.path_or_inline_dv = signed_deletion_vector_url.url().to_string();
                        }
                        "p" => {
                            let deletion_vector_url = dv.path_or_inline_dv.clone();
                            let signed_dv = signer.sign_url(&deletion_vector_url).await;

                            dv.path_or_inline_dv = signed_dv.url().to_string();
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, time::Duration};

    use chrono::{TimeZone, Utc};
    use delta_kernel::actions::DeletionVectorDescriptor;
    use delta_kernel::actions::Format;
    use insta::assert_json_snapshot;

    use crate::signer::{MockUrlSigner, SignedUrl};

    use super::*;

    #[test]
    fn build_delta_protocol_line() {
        let protocol = Protocol {
            min_reader_version: 1,
            min_writer_version: 2,
            reader_features: None,
            writer_features: None,
        };

        let protocol_line = ProtocolResponseLine::from_protocol(protocol);
        let line = DeltaResponseLine::Protocol(protocol_line);

        assert_json_snapshot!(line);
    }

    #[test]
    fn build_delta_metadata_line() {
        let metadata = Metadata {
            id: String::from("f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2"),
            name: Some(String::from("name")),
            description: Some(String::from("description")),
            format: Format {
                provider: String::from("parquet"),
                options: HashMap::new(),
            },
            schema_string: String::from("{\"type\":\"struct\",\"fields\":[{\"name\":\"eventTime\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}}]}"),
            partition_columns: vec![String::from("date")],
            created_time: Some(1619824428000),
            configuration: HashMap::from_iter([("opt1".to_owned(), Some("true".to_owned()))]),
        };

        let metadata_line =
            MetadataResponseLine::from_metadata_with_opts(metadata, Some(1), Some(2), Some(3));
        let line = DeltaResponseLine::Metadata(metadata_line);

        assert_json_snapshot!(line);
    }

    #[test]
    fn build_delta_file_line() {
        let add = Add {
            path: "https://bucket/key".to_owned(),
            partition_values: HashMap::from_iter([(
                "date".to_owned(),
                Some("2021-04-28".to_owned()),
            )]),
            size: 573,
            stats: Some("{\"numRecords\":1}".to_owned()),
            modification_time: 1619824428000,
            data_change: false,
            tags: HashMap::new(),
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
        };

        let file_line = FileResponseLine::from_add_with_opts(add, Some(1), Some(2), Some(3));
        let line = DeltaResponseLine::File(file_line);

        assert_json_snapshot!(line);
    }

    #[tokio::test]
    async fn sign_delta_file_line_without_deletion_vector() {
        let mut mock_signer = MockUrlSigner::new();
        mock_signer.expect_sign_url().returning(|s| {
            let url = format!("{s}?sig=foo_signature");
            let valid_from = Utc.timestamp_opt(1_610_000_000, 0).unwrap();
            let expiration = Duration::from_secs(3600);
            SignedUrl::new(url, valid_from, expiration)
        });

        let add = Add {
            path: "key.snappy.parquet".to_owned(),
            partition_values: HashMap::new(),
            size: 573,
            stats: None,
            modification_time: 1619824428000,
            data_change: false,
            tags: HashMap::new(),
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
        };

        let mut file_line = FileResponseLine::from_add_with_opts(add, None, None, None);
        let table_root = "https://bucket/prefix";
        let signer = Arc::new(mock_signer);
        file_line.sign(table_root, signer).await;

        assert_json_snapshot!(file_line);
    }

    #[tokio::test]
    async fn sign_delta_file_line_inline_deletion_vector() {
        let mut mock_signer = MockUrlSigner::new();
        mock_signer.expect_sign_url().returning(|s| {
            let url = format!("{s}?sig=foo_signature");
            let valid_from = Utc.timestamp_opt(1_610_000_000, 0).unwrap();
            let expiration = Duration::from_secs(3600);
            SignedUrl::new(url, valid_from, expiration)
        });

        let add = Add {
            path: "key.snappy.parquet".to_owned(),
            partition_values: HashMap::new(),
            size: 573,
            stats: None,
            modification_time: 1619824428000,
            data_change: false,
            tags: HashMap::new(),
            deletion_vector: Some(DeletionVectorDescriptor {
                storage_type: "i".to_string(),
                path_or_inline_dv: "wi5b=000010000siXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L".to_string(),
                offset: None,
                size_in_bytes: 40,
                cardinality: 6,
            }),
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
        };

        let mut file_line = FileResponseLine::from_add_with_opts(add, None, None, None);
        let table_root = "https://bucket/prefix";
        let signer = Arc::new(mock_signer);
        file_line.sign(table_root, signer).await;

        assert_json_snapshot!(file_line);
    }

    #[tokio::test]
    async fn sign_delta_file_line_relative_deletion_vector() {
        let mut mock_signer = MockUrlSigner::new();
        mock_signer.expect_sign_url().returning(|s| {
            let url = format!("{s}?sig=foo_signature");
            let valid_from = Utc.timestamp_opt(1_610_000_000, 0).unwrap();
            let expiration = Duration::from_secs(3600);
            SignedUrl::new(url, valid_from, expiration)
        });

        let add = Add {
            path: "key.snappy.parquet".to_owned(),
            partition_values: HashMap::new(),
            size: 573,
            stats: None,
            modification_time: 1619824428000,
            data_change: false,
            tags: HashMap::new(),
            deletion_vector: Some(DeletionVectorDescriptor {
                storage_type: "u".to_string(),
                path_or_inline_dv: "ab^-aqEH.-t@S}K{vb[*k^".to_string(),
                offset: Some(4),
                size_in_bytes: 40,
                cardinality: 6,
            }),
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
        };

        let mut file_line = FileResponseLine::from_add_with_opts(add, None, None, None);
        let table_root = "https://bucket/prefix";
        let signer = Arc::new(mock_signer);
        file_line.sign(table_root, signer).await;

        assert_json_snapshot!(file_line);
    }

    #[tokio::test]
    async fn sign_delta_file_line_absolute_deletion_vector() {
        let mut mock_signer = MockUrlSigner::new();
        mock_signer.expect_sign_url().returning(|s| {
            let url = format!("{s}?sig=foo_signature");
            let valid_from = Utc.timestamp_opt(1_610_000_000, 0).unwrap();
            let expiration = Duration::from_secs(3600);
            SignedUrl::new(url, valid_from, expiration)
        });

        let add = Add {
            path: "key.snappy.parquet".to_owned(),
            partition_values: HashMap::new(),
            size: 573,
            stats: None,
            modification_time: 1619824428000,
            data_change: false,
            tags: HashMap::new(),
            deletion_vector: Some(DeletionVectorDescriptor {
                storage_type: "p".to_string(),
                path_or_inline_dv:
                    "s3://mytable/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin"
                        .to_string(),
                offset: Some(4),
                size_in_bytes: 40,
                cardinality: 6,
            }),
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
        };

        let mut file_line = FileResponseLine::from_add_with_opts(add, None, None, None);
        let table_root = "https://bucket/prefix";
        let signer = Arc::new(mock_signer);
        file_line.sign(table_root, signer).await;

        assert_json_snapshot!(file_line);
    }
}
