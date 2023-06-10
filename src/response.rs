use std::io::Write;

use axum::response::Response;
use axum::{
    http::{header, StatusCode},
    response::IntoResponse,
    Json,
};
use bytes::{BufMut, BytesMut};
use serde::Serialize;

use crate::protocol::action::{Metadata, Protocol};
use crate::protocol::securable::{Schema, Share, Table};
use crate::protocol::share::List;
use crate::protocol::table::{SignedDataFile, SignedTableData, TableMetadata, TableVersionNumber};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListSharesResponse {
    items: Vec<Share>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_page_token: Option<String>,
}

impl From<List<Share>> for ListSharesResponse {
    fn from(value: List<Share>) -> Self {
        Self {
            items: value.items().to_vec(),
            next_page_token: value.next_page_token().cloned(),
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

impl From<Share> for GetShareResponse {
    fn from(value: Share) -> Self {
        Self { share: value }
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
#[serde(rename_all = "camelCase")]
struct SchemaItem {
    name: String,
    share: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListSchemasResponse {
    items: Vec<SchemaItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_page_token: Option<String>,
}

impl From<List<Schema>> for ListSchemasResponse {
    fn from(value: List<Schema>) -> Self {
        Self {
            items: value
                .items()
                .iter()
                .map(|item| SchemaItem {
                    name: item.name().to_owned(),
                    share: item.share_name().to_owned(),
                })
                .collect(),
            next_page_token: value.next_page_token().cloned(),
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
struct TableItem {
    name: String,
    schema: String,
    share: String,
    share_id: Option<String>,
    id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListTablesResponse {
    items: Vec<TableItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_page_token: Option<String>,
}

impl From<List<Table>> for ListTablesResponse {
    fn from(value: List<Table>) -> Self {
        Self {
            items: value
                .items()
                .iter()
                .map(|t| TableItem {
                    name: t.name().to_owned(),
                    schema: t.schema_name().to_owned(),
                    share: t.share_name().to_owned(),
                    share_id: t.share_id().map(|s| s.to_owned()),
                    id: t.id().map(|s| s.to_owned()),
                })
                .collect::<Vec<_>>(),
            next_page_token: value.next_page_token().cloned(),
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
    version: u64,
}

impl From<TableVersionNumber> for TableVersionResponse {
    fn from(version: TableVersionNumber) -> Self {
        Self { version }
    }
}

impl IntoResponse for TableVersionResponse {
    fn into_response(self) -> Response {
        (StatusCode::OK, [("Delta-Table-Version", self.version)]).into_response()
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
pub struct TableInfoResponse {
    version: TableVersionNumber,
    lines: Vec<JsonWrapper>,
}

impl IntoResponse for TableInfoResponse {
    fn into_response(self) -> Response {
        let mut buf = BytesMut::new().writer();
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

impl From<TableMetadata> for TableInfoResponse {
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

impl From<SignedTableData> for TableInfoResponse {
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
