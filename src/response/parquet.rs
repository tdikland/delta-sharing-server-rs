use axum::response::{IntoResponse, Response};
use bytes::{BufMut, BytesMut};
use delta_kernel::actions::{Add, Format, Metadata, Protocol};
use http::{header, StatusCode};
use serde::Serialize;
use std::{collections::HashMap, io::Write, sync::Arc};

use crate::{
    reader::{TableData, TableMeta},
    signer::UrlSigner,
};

#[derive(Debug, Clone, Serialize)]
pub struct ParquetResponse {
    version: u64,
    protocol: ParquetResponseLine,
    metadata: ParquetResponseLine,
    lines: Vec<ParquetResponseLine>,
}

impl ParquetResponse {
    pub async fn sign(mut self, table_root: &str, signer: Arc<dyn UrlSigner>) -> Self {
        for line in self.lines.iter_mut() {
            line.sign(table_root, signer.clone()).await;
        }
        self
    }
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

impl From<TableMeta> for ParquetResponse {
    fn from(meta: TableMeta) -> Self {
        let protocol = ProtocolLine::from_protocol(meta.protocol().clone().into_inner());
        let metadata = MetadataLine::from_metadata_with_opts(
            meta.metadata().clone().into_inner(),
            None,
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

impl From<TableData> for ParquetResponse {
    fn from(data: TableData) -> Self {
        let protocol = ProtocolLine::from_protocol(data.protocol().clone().into_inner());
        let metadata = MetadataLine::from_metadata_with_opts(
            data.metadata().clone().into_inner(),
            None,
            None,
            None,
        );

        let mut lines = vec![];
        for file in data.data().into_iter().cloned() {
            lines.push(FileLine::from_add_with_opts(file.into_inner(), None, None, None).into());
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
enum ParquetResponseLine {
    Protocol(ProtocolLine),
    #[serde(rename = "metaData")]
    Metadata(MetadataLine),
    File(FileLine),
}

impl ParquetResponseLine {
    async fn sign(&mut self, table_root: &str, signer: Arc<dyn UrlSigner>) {
        match self {
            Self::File(file) => {
                file.sign(table_root, signer).await;
            }
            _ => {}
        }
    }
}

impl From<ProtocolLine> for ParquetResponseLine {
    fn from(protocol: ProtocolLine) -> Self {
        Self::Protocol(protocol)
    }
}

impl From<MetadataLine> for ParquetResponseLine {
    fn from(metadata: MetadataLine) -> Self {
        Self::Metadata(metadata)
    }
}

impl From<FileLine> for ParquetResponseLine {
    fn from(file: FileLine) -> Self {
        Self::File(file)
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProtocolLine {
    min_reader_version: u32,
}

impl ProtocolLine {
    fn from_protocol(protocol: Protocol) -> Self {
        let min_reader_version = u32::try_from(protocol.min_reader_version)
            .expect("protocol min_reader_version is non-negative");

        Self { min_reader_version }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ParquetResponseFormat {
    provider: String,
}

impl From<Format> for ParquetResponseFormat {
    fn from(format: Format) -> Self {
        Self {
            provider: format.provider,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct MetadataLine {
    id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    format: ParquetResponseFormat,
    schema_string: String,
    partition_columns: Vec<String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    configuration: HashMap<String, Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    num_files: Option<u64>,
}

impl MetadataLine {
    fn from_metadata_with_opts(
        metadata: Metadata,
        version: Option<u64>,
        size: Option<u64>,
        num_files: Option<u64>,
    ) -> Self {
        Self {
            id: metadata.id,
            name: metadata.name,
            description: metadata.description,
            format: metadata.format.into(),
            schema_string: metadata.schema_string,
            partition_columns: metadata.partition_columns,
            configuration: metadata.configuration,
            version,
            size,
            num_files,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct FileLine {
    url: String,
    id: String,
    partition_values: HashMap<String, Option<String>>,
    size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    stats: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    timestamp: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expiration_timestamp: Option<i64>,
}

impl FileLine {
    fn from_add_with_opts(
        add: Add,
        version: Option<u64>,
        timestamp: Option<u64>,
        expiration_timestamp: Option<i64>,
    ) -> Self {
        let file_id = format!("{:x}", md5::compute(add.path.as_bytes()));
        let size = u64::try_from(add.size).expect("file size is non-negative");
        Self {
            url: add.path,
            id: file_id,
            partition_values: add.partition_values,
            size,
            stats: add.stats,
            version,
            timestamp,
            expiration_timestamp,
        }
    }

    async fn sign(&mut self, root_path: &str, signer: Arc<dyn UrlSigner>) {
        let file_url = format!("{}/{}", root_path, self.url);
        let signed_url = signer.sign_url(&file_url).await;

        self.url = signed_url.url().to_string();
        self.expiration_timestamp = Some(signed_url.expires_at().timestamp_millis());
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use chrono::{TimeZone, Utc};
    use delta_kernel::actions::Format;
    use insta::assert_json_snapshot;
    use mockall::predicate::eq;

    use crate::signer::{MockUrlSigner, SignedUrl};

    use super::*;

    #[test]
    fn build_protocol_parquet_line() {
        let protocol = Protocol {
            min_reader_version: 1,
            min_writer_version: 2,
            reader_features: None,
            writer_features: None,
        };

        let protocol_line = ProtocolLine::from_protocol(protocol);
        let line = ParquetResponseLine::Protocol(protocol_line);

        assert_json_snapshot!(line);
    }

    #[test]
    fn build_metadata_parquet_line() {
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

        let mline = MetadataLine::from_metadata_with_opts(metadata, Some(1), Some(123456), Some(5));
        let line = ParquetResponseLine::Metadata(mline);

        assert_json_snapshot!(line);
    }

    #[test]
    fn build_file_parquet_line() {
        let add = Add {
            path: "key.snappy.parquet".to_owned(),
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

        let file_line =
            FileLine::from_add_with_opts(add, Some(1), Some(1619824428000), Some(1619824430000));
        let line = ParquetResponseLine::File(file_line);
        assert_json_snapshot!(line);
    }

    #[tokio::test]
    async fn sign_file_response_line() {
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
        let mut file_line = FileLine::from_add_with_opts(add, None, None, None);

        let mut mock_signer = MockUrlSigner::new();
        mock_signer
            .expect_sign_url()
            .with(eq("s3://bucket/prefix/key.snappy.parquet"))
            .once()
            .returning(|_| {
                let url = String::from("s3://bucket/prefix/key.snappy.parquet?sig=foo_signature");
                let valid_from = Utc.timestamp_opt(1_610_000_000, 0).unwrap();
                let expiration = Duration::from_secs(3600);
                SignedUrl::new(url, valid_from, expiration)
            });

        file_line
            .sign("s3://bucket/prefix", Arc::new(mock_signer))
            .await;

        assert_eq!(
            file_line.url,
            "s3://bucket/prefix/key.snappy.parquet?sig=foo_signature"
        );
        assert_eq!(file_line.expiration_timestamp, Some(1_610_003_600_000));
        assert_json_snapshot!(file_line);
    }
}
