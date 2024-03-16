use axum::response::{IntoResponse, Response};
use bytes::{BufMut, BytesMut};
use deltalake::kernel::{Add, AddCDCFile, Format, Metadata, Protocol, Remove};
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
            protocol: ParquetResponseLine::Protocol(table_metadata.protocol.clone().into()),
            metadata: ParquetResponseLine::Metadata(MetadataParquetLine::from_metadata_with_opts(
                table_metadata.metadata,
                None,
                None,
                None,
            )),
            lines: vec![],
        }
    }
}

impl From<SignedTableData> for ParquetResponse {
    fn from(signed_table_data: SignedTableData) -> Self {
        let lines = signed_table_data.data.into_iter().map(Into::into).collect();

        Self {
            version: signed_table_data.version,
            protocol: ParquetResponseLine::Protocol(signed_table_data.protocol.clone().into()),
            metadata: ParquetResponseLine::Metadata(MetadataParquetLine::from_metadata_with_opts(
                signed_table_data.metadata,
                None,
                None,
                None,
            )),
            lines,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
enum ParquetResponseLine {
    Protocol(ProtocolParquetLine),
    #[serde(rename = "metaData")]
    Metadata(MetadataParquetLine),
    File(FileParquetLine),
    Add(AddParquetLine),
    Cdf(CdfParquetLine),
    Remove(RemoveParquetLine),
}

impl From<SignedDataFile> for ParquetResponseLine {
    fn from(signed_data_file: SignedDataFile) -> Self {
        match signed_data_file {
            SignedDataFile::File(file) => ParquetResponseLine::File(
                FileParquetLine::from_add_with_opts(file, None, None, None),
            ),
            SignedDataFile::Add(add) => ParquetResponseLine::Add(add.into()),
            SignedDataFile::Cdf(cdf) => ParquetResponseLine::Cdf(cdf.into()),
            SignedDataFile::Remove(remove) => ParquetResponseLine::Remove(remove.into()),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProtocolParquetLine {
    min_reader_version: u32,
}

impl ProtocolParquetLine {
    fn from_protocol(protocol: Protocol) -> Self {
        Self {
            min_reader_version: u32::try_from(protocol.min_reader_version)
                .expect("protocol min_reader_version is non-negative"),
        }
    }
}

impl From<Protocol> for ProtocolParquetLine {
    fn from(protocol: Protocol) -> Self {
        Self::from_protocol(protocol)
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ParquetResponseFormat {
    provider: String,
}

impl Default for ParquetResponseFormat {
    fn default() -> Self {
        Self {
            provider: "parquet".to_owned(),
        }
    }
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
struct MetadataParquetLine {
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

impl MetadataParquetLine {
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
            format: ParquetResponseFormat::from(metadata.format),
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
struct FileParquetLine {
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
    expiration_timestamp: Option<u64>,
}

impl FileParquetLine {
    fn from_add_with_opts(
        add: Add,
        version: Option<u64>,
        timestamp: Option<u64>,
        expiration_timestamp: Option<u64>,
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
}

impl From<Add> for FileParquetLine {
    fn from(add: Add) -> Self {
        let file_id = format!("{:x}", md5::compute(add.path.as_bytes()));
        Self {
            url: add.path,
            id: file_id,
            partition_values: add.partition_values,
            size: add.size as u64,
            stats: add.stats,
            version: None,
            timestamp: None,
            expiration_timestamp: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
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
    fn from(_add: Add) -> Self {
        todo!()
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
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
    fn from(_add_cdc_file: AddCDCFile) -> Self {
        todo!()
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
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
    fn from(_remove: Remove) -> Self {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use insta::assert_json_snapshot;

    use super::*;

    #[test]
    fn protocol_parquet_line() {
        let protocol = Protocol {
            min_reader_version: 1,
            min_writer_version: 2,
            reader_features: None,
            writer_features: None,
        };

        let protocol_line = ProtocolParquetLine::from_protocol(protocol);
        assert_eq!(protocol_line.min_reader_version, 1);

        let line = ParquetResponseLine::Protocol(protocol_line);
        assert_json_snapshot!(line);
    }

    #[test]
    fn metadata_parquet_line() {
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

        let mline =
            MetadataParquetLine::from_metadata_with_opts(metadata, Some(1), Some(123456), Some(5));
        assert_eq!(mline.id, "f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2");
        assert_eq!(mline.name, Some("name".to_owned()));
        assert_eq!(mline.description, Some("description".to_owned()));
        assert_eq!(mline.format.provider, "parquet");
        assert_eq!(mline.schema_string, "{\"type\":\"struct\",\"fields\":[{\"name\":\"eventTime\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}}]}");
        assert_eq!(mline.partition_columns, vec!["date".to_owned()]);
        assert_eq!(mline.configuration.len(), 1);
        assert_eq!(mline.configuration["opt1"].as_deref(), Some("true"));
        assert_eq!(mline.version, Some(1));
        assert_eq!(mline.size, Some(123456));
        assert_eq!(mline.num_files, Some(5));

        let line = ParquetResponseLine::Metadata(mline);
        assert_json_snapshot!(line);
    }

    #[test]
    fn file_parquet_line() {
        let add = Add {
            path: "https://bucket/key?sig=foo".to_owned(),
            partition_values: HashMap::from_iter([(
                "date".to_owned(),
                Some("2021-04-28".to_owned()),
            )]),
            size: 573,
            stats: Some("{\"numRecords\":1}".to_owned()),
            modification_time: 1619824428000,
            data_change: false,
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
            stats_parsed: None,
        };

        let fline = FileParquetLine::from_add_with_opts(
            add,
            Some(1),
            Some(1619824428000),
            Some(1619824430000),
        );
        assert_eq!(fline.url, "https://bucket/key?sig=foo");
        assert_eq!(fline.id, "e4250277491e245118e0d72a3287d385");
        assert_eq!(fline.partition_values.len(), 1);
        assert_eq!(
            fline.partition_values["date"],
            Some("2021-04-28".to_owned())
        );
        assert_eq!(fline.size, 573);
        assert_eq!(fline.stats, Some("{\"numRecords\":1}".to_owned()));
        assert_eq!(fline.version, Some(1));
        assert_eq!(fline.timestamp, Some(1619824428000));
        assert_eq!(fline.expiration_timestamp, Some(1619824430000));

        let line = ParquetResponseLine::File(fline);
        assert_json_snapshot!(line);
    }
}
