use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io::Write;

use axum::http::HeaderName;
use axum::response::Response;
use axum::BoxError;
use axum::{
    http::{header, StatusCode},
    response::IntoResponse,
    Json,
};
use bytes::{BufMut, BytesMut};
use futures_util::stream::TryStreamExt;
use serde::Serialize;

use crate::catalog::{Page, SchemaInfo, ShareInfo, TableInfo};
use crate::protocol::action::{Metadata, Protocol};
use crate::protocol::table::{SignedDataFile, SignedTableData, TableMetadata, TableVersionNumber};

const DELTA_TABLE_VERSION: HeaderName = HeaderName::from_static("delta-table-version");

#[derive(Debug, Clone, Serialize)]
pub struct Share {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
}

impl From<ShareInfo> for Share {
    fn from(value: ShareInfo) -> Self {
        Self {
            name: value.name,
            id: value.id,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListSharesResponse {
    items: Vec<Share>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_page_token: Option<String>,
}

impl From<Page<ShareInfo>> for ListSharesResponse {
    fn from(value: Page<ShareInfo>) -> Self {
        let (items, next_page_token) = value.into_parts();

        Self {
            items: items.into_iter().map(Share::from).collect(),
            next_page_token,
        }
    }
}

impl IntoResponse for ListSharesResponse {
    fn into_response(self) -> Response {
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json; charset=utf-8")],
            Json(self),
        )
            .into_response()
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetShareResponse {
    share: Share,
}

impl From<ShareInfo> for GetShareResponse {
    fn from(value: ShareInfo) -> Self {
        Self {
            share: value.into(),
        }
    }
}

impl IntoResponse for GetShareResponse {
    fn into_response(self) -> Response {
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json; charset=utf-8")],
            Json(self),
        )
            .into_response()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Schema {
    name: String,
    share: String,
}

impl From<SchemaInfo> for Schema {
    fn from(value: SchemaInfo) -> Self {
        Self {
            name: value.name,
            share: value.share_name,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListSchemasResponse {
    items: Vec<Schema>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_page_token: Option<String>,
}

impl From<Page<SchemaInfo>> for ListSchemasResponse {
    fn from(value: Page<SchemaInfo>) -> Self {
        let (items, next_page_token) = value.into_parts();

        Self {
            items: items.into_iter().map(Schema::from).collect(),
            next_page_token,
        }
    }
}

impl IntoResponse for ListSchemasResponse {
    fn into_response(self) -> Response {
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json; charset=utf-8")],
            Json(self),
        )
            .into_response()
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Table {
    name: String,
    schema: String,
    share: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    share_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
}

impl From<TableInfo> for Table {
    fn from(value: TableInfo) -> Self {
        Self {
            name: value.name,
            schema: value.schema_name,
            share: value.share_name,
            share_id: value.share_id,
            id: value.id,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListTablesResponse {
    items: Vec<Table>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_page_token: Option<String>,
}

impl From<Page<TableInfo>> for ListTablesResponse {
    fn from(value: Page<TableInfo>) -> Self {
        let (items, next_page_token) = value.into_parts();

        Self {
            items: items.into_iter().map(Table::from).collect(),
            next_page_token,
        }
    }
}

impl IntoResponse for ListTablesResponse {
    fn into_response(self) -> Response {
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json; charset=utf-8")],
            Json(self),
        )
            .into_response()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TableVersionResponse {
    version: TableVersionNumber,
}

impl From<TableVersionNumber> for TableVersionResponse {
    fn from(version: TableVersionNumber) -> Self {
        Self { version }
    }
}

impl IntoResponse for TableVersionResponse {
    fn into_response(self) -> Response {
        (StatusCode::OK, [(DELTA_TABLE_VERSION, self.version)]).into_response()
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum JsonWrapper {
    Protocol(Protocol),
    #[serde(rename = "metaData")]
    Metadata(Metadata),
    File(SignedDataFile),
    // Add(SignedChangeFile),
}

#[derive(Debug, Clone, Serialize)]
pub struct TableActionsResponse {
    version: TableVersionNumber,
    lines: Vec<JsonWrapper>,
}

#[derive(Debug, Clone, Serialize)]
struct StreamError;

impl Display for StreamError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "StreamError")
    }
}

impl Error for StreamError {}

impl IntoResponse for TableActionsResponse {
    fn into_response(self) -> Response {
        let raw_stream =
            futures::stream::iter(self.lines.into_iter().map(Ok::<JsonWrapper, StreamError>));
        let _stream = raw_stream.map_err(Into::into).and_then(|value| async move {
            let mut buf = BytesMut::new().writer();
            serde_json::to_writer(&mut buf, &value)?;
            buf.write_all(b"\n")?;
            Ok::<_, BoxError>(buf.into_inner().freeze())
        });

        todo!()
        // let stream = Body::wrap_stream(stream);
        // let version = self.version.to_string();

        // let mut headers = HeaderMap::new();
        // headers.insert(
        //     header::CONTENT_TYPE,
        //     "application/x-ndjson; charset=utf-8".parse().unwrap(),
        // );
        // headers.insert("Delta-Table-Version", version.parse().unwrap());
        // let mut response = Response::new(stream);
        // *response.headers_mut() = headers;

        // response.into_response()
    }
}

// impl IntoResponse for TableActionsResponse {
//     fn into_response(self) -> Response {
//         let mut buf = BytesMut::new().writer();
//         for line in self.lines {
//             serde_json::to_writer(&mut buf, &line).unwrap();
//             buf.write_all(b"\n").unwrap();
//         }

//         let version = self.version.to_string();
//         let headers = [
//             (
//                 header::CONTENT_TYPE.as_str(),
//                 "application/x-ndjson; charset=utf-8",
//             ),
//             ("Delta-Table-Version", version.as_ref()),
//         ];

//         (StatusCode::OK, headers, buf.into_inner()).into_response()
//     }
// }

#[derive(Debug, Clone, Serialize)]
pub struct ParquetProtocol {
    min_reader_version: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct ParquetMetadata {
    id: String,
    name: Option<String>,
    description: Option<String>,
    format: String, // TODO: format
    schema_string: String,
    partition_columns: Vec<String>,
    configuration: HashMap<String, String>,
    version: Option<u64>,
    size: Option<u64>,
    num_files: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ParquetFile {
    path: String,
    id: String,
    partition_values: Vec<String>,
    size: u64,
    stats: Option<String>,
    version: Option<u64>,
    timestamp: Option<u64>,
    expiration_timestamp: Option<u64>,
}

// #[derive(Debug, Clone, Serialize)]
// pub enum ParquetAction {
//     /// Protocol action
//     Protocol(ParquetProtocol),
//     /// Metadata action
//     Metadata(ParquetMetadata),
//     /// Data action
//     File(ParquetFile),
// }

// #[derive(Debug, Clone, Serialize)]
// pub struct ParquetSharingResponse {
//     version: TableVersionNumber,
//     protocol: ParquetAction,
//     metadata: ParquetAction,
//     data: Vec<ParquetAction>,
// }

// impl IntoResponse for ParquetSharingResponse {
//     fn into_response(self) -> Response {
//         let mut buf = BytesMut::new().writer();
//         serde_json::to_writer(&mut buf, &self.protocol).unwrap();
//         buf.write_all(b"\n").unwrap();
//         serde_json::to_writer(&mut buf, &self.metadata).unwrap();
//         buf.write_all(b"\n").unwrap();
//         for file in self.data {
//             serde_json::to_writer(&mut buf, &file).unwrap();
//             buf.write_all(b"\n").unwrap();
//         }

//         let version = self.version.to_string();
//         let headers = [
//             (CONTENT_TYPE, "application/x-ndjson; charset=utf-8"),
//             (DELTA_TABLE_VERSION, version.as_ref()),
//         ];

//         (StatusCode::OK, headers, buf.into_inner()).into_response()
//     }
// }

// pub struct DeltaResponse {
//     version: TableVersionNumber,
//     protocol: Protocol,
//     metadata: Metadata,
//     data: Vec<SignedDataFile>,
// }

impl From<TableMetadata> for TableActionsResponse {
    fn from(v: TableMetadata) -> Self {
        let lines = vec![
            JsonWrapper::Protocol(v.protocol),
            JsonWrapper::Metadata(v.metadata),
        ];

        Self {
            version: v.version,
            lines,
        }
    }
}

impl From<SignedTableData> for TableActionsResponse {
    fn from(value: SignedTableData) -> Self {
        let mut lines = vec![];
        lines.push(JsonWrapper::Protocol(value.protocol.clone()));
        lines.push(JsonWrapper::Metadata(value.metadata.clone()));
        for f in value.data.clone() {
            lines.push(JsonWrapper::File(f.clone()))
        }

        Self {
            version: value.version,
            lines,
        }
    }
}

#[cfg(test)]
mod test {
    use axum::{
        body::to_bytes,
        http::{header::CONTENT_TYPE, HeaderValue},
    };
    use bytes::Bytes;

    use super::*;

    #[tokio::test]
    async fn list_shares_response() {
        let share_info = Page::new(
            vec![ShareInfo::new("foo".to_owned(), Some("foo_id".to_owned()))],
            Some("bar".to_owned()),
        );
        let response = ListSharesResponse::from(share_info);
        let res = response.into_response();

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers().get(CONTENT_TYPE).unwrap(),
            HeaderValue::from_static("application/json; charset=utf-8")
        );
        assert_eq!(
            to_bytes(res.into_body(), 100).await.unwrap(),
            Bytes::from(r#"{"items":[{"name":"foo","id":"foo_id"}],"nextPageToken":"bar"}"#)
        );
    }

    #[tokio::test]
    async fn get_share_response() {
        let share_info = ShareInfo::new("foo".to_owned(), Some("foo_id".to_owned()));
        let response = GetShareResponse::from(share_info);
        let res = response.into_response();

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers().get(CONTENT_TYPE).unwrap(),
            HeaderValue::from_static("application/json; charset=utf-8")
        );
        assert_eq!(
            to_bytes(res.into_body(), 100).await.unwrap(),
            Bytes::from(r#"{"share":{"name":"foo","id":"foo_id"}}"#)
        );
    }

    #[tokio::test]
    async fn list_schemas_response() {
        let schema_info = Page::new(
            vec![SchemaInfo::new("foo".to_owned(), "bar".to_owned())],
            Some("token".to_owned()),
        );
        let response = ListSchemasResponse::from(schema_info);
        let res = response.into_response();

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers().get(CONTENT_TYPE).unwrap(),
            HeaderValue::from_static("application/json; charset=utf-8")
        );
        assert_eq!(
            to_bytes(res.into_body(), 100).await.unwrap(),
            Bytes::from(r#"{"items":[{"name":"foo","share":"bar"}],"nextPageToken":"token"}"#)
        );
    }

    #[tokio::test]
    async fn list_tables_response() {
        let table_info = Page::new(
            vec![TableInfo::new(
                "foo".to_owned(),
                "bar".to_owned(),
                "baz".to_owned(),
                "table_location".to_owned(),
            )],
            Some("token".to_owned()),
        );
        let response = ListTablesResponse::from(table_info);
        let res = response.into_response();

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers().get(CONTENT_TYPE).unwrap(),
            HeaderValue::from_static("application/json; charset=utf-8")
        );
        assert_eq!(
            to_bytes(res.into_body(), 100).await.unwrap(),
            Bytes::from(
                r#"{"items":[{"name":"foo","schema":"bar","share":"baz"}],"nextPageToken":"token"}"#
            )
        );
    }

    #[tokio::test]
    async fn table_version_response() {
        let response = TableVersionResponse { version: 123 };
        let res = response.into_response();

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers().get("Delta-Table-Version").unwrap(),
            HeaderValue::from_static("123")
        );
    }

    //     #[tokio::test]
    //     async fn parquet_response() {
    //         let response = ParquetSharingResponse {
    //             version: 123,
    //             protocol: ProtocolBuilder::new().min_reader_version(1).build(),
    //             metadata: MetadataBuilder::new("id", "schema_str").build(),
    //             data: vec![],
    //         };
    //         let res = response.into_response();

    //         assert_eq!(res.status(), StatusCode::OK);
    //         assert_eq!(
    //             res.headers().get(CONTENT_TYPE).unwrap(),
    //             HeaderValue::from_static("application/x-ndjson; charset=utf-8")
    //         );
    //         assert_eq!(
    //             res.headers().get("Delta-Table-Version").unwrap(),
    //             HeaderValue::from_static("123")
    //         );
    //         assert_eq!(
    //             to_bytes(res.into_body(), 999).await.unwrap(),
    //             Bytes::from(
    //                 r#"{"minReaderVersion":1}
    // {"id":"id","format":{"provider":"parquet"},"schemaString":"schema_str","partitionColumns":[]}
    // "#
    //             )
    //         );
    //     }
}
