use std::sync::Arc;

use axum::http::HeaderName;
use axum::response::Response;
use axum::{
    http::{header, StatusCode},
    response::IntoResponse,
    Json,
};
use serde::Serialize;

use self::delta::DeltaResponse;
use self::parquet::ParquetResponse;
use crate::catalog::{Page, Schema as SchemaInfo, Share as ShareInfo, Table as TableInfo};
use crate::reader::TableVersionNumber;
use crate::signer::UrlSigner;

pub mod delta;
pub mod parquet;

static DELTA_TABLE_VERSION: HeaderName = HeaderName::from_static("delta-table-version");

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Share {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
}

impl From<ShareInfo> for Share {
    fn from(value: ShareInfo) -> Self {
        Self {
            name: value.name().to_owned(),
            id: value.id().map(|id| id.to_owned()),
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
#[serde(rename_all = "camelCase")]
pub struct Schema {
    name: String,
    share: String,
}

impl From<SchemaInfo> for Schema {
    fn from(value: SchemaInfo) -> Self {
        Self {
            name: value.name().to_owned(),
            share: value.share_name().to_owned(),
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
            name: value.name().to_owned(),
            schema: value.schema_name().to_owned(),
            share: value.share_name().to_owned(),
            share_id: value.share_id().map(ToOwned::to_owned),
            id: value.id().to_owned().map(ToOwned::to_owned),
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
        (
            StatusCode::OK,
            [(DELTA_TABLE_VERSION.clone(), self.version.version())],
        )
            .into_response()
    }
}

#[derive(Debug, Clone)]
pub enum TableActionsResponse {
    Parquet(ParquetResponse),
    Delta(DeltaResponse),
}

impl TableActionsResponse {
    pub fn new_parquet<R: Into<ParquetResponse>>(response: R) -> Self {
        Self::Parquet(response.into())
    }

    pub fn new_delta<R: Into<DeltaResponse>>(response: R) -> Self {
        Self::Delta(response.into())
    }

    pub async fn sign(self, table_root: &str, signer: Arc<dyn UrlSigner>) -> Self {
        match self {
            TableActionsResponse::Parquet(response) => {
                let signed_response = response.sign(table_root, signer).await;
                TableActionsResponse::Parquet(signed_response)
            }
            TableActionsResponse::Delta(mut response) => {
                response.sign(table_root, signer).await;
                TableActionsResponse::Delta(response)
            }
        }
    }
}

impl IntoResponse for TableActionsResponse {
    fn into_response(self) -> Response {
        match self {
            TableActionsResponse::Parquet(response) => response.into_response(),
            TableActionsResponse::Delta(response) => response.into_response(),
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
            vec![ShareInfo::builder()
                .name("share_name")
                .id("share_id")
                .build()
                .unwrap()],
            Some("page_token".to_owned()),
        );
        let response = ListSharesResponse::from(share_info);
        let res = response.into_response();

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers().get(CONTENT_TYPE).unwrap(),
            HeaderValue::from_static("application/json; charset=utf-8")
        );
        assert_eq!(
            to_bytes(res.into_body(), 1000).await.unwrap(),
            Bytes::from(
                r#"{"items":[{"name":"share_name","id":"share_id"}],"nextPageToken":"page_token"}"#
            )
        );
    }

    #[tokio::test]
    async fn get_share_response() {
        let share_info = ShareInfo::builder()
            .name("share_name")
            .id("share_id")
            .build()
            .unwrap();
        let response = GetShareResponse::from(share_info);
        let res = response.into_response();

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers().get(CONTENT_TYPE).unwrap(),
            HeaderValue::from_static("application/json; charset=utf-8")
        );
        assert_eq!(
            to_bytes(res.into_body(), 1000).await.unwrap(),
            Bytes::from(r#"{"share":{"name":"share_name","id":"share_id"}}"#)
        );
    }

    #[tokio::test]
    async fn list_schemas_response() {
        let schema_info = Page::new(
            vec![SchemaInfo::builder()
                .name("schema_name")
                .share_name("share_name")
                .build()
                .unwrap()],
            Some("page_token".to_owned()),
        );
        let response = ListSchemasResponse::from(schema_info);
        let res = response.into_response();

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers().get(CONTENT_TYPE).unwrap(),
            HeaderValue::from_static("application/json; charset=utf-8")
        );
        assert_eq!(
            to_bytes(res.into_body(), 1000).await.unwrap(),
            Bytes::from(
                r#"{"items":[{"name":"schema_name","share":"share_name"}],"nextPageToken":"page_token"}"#
            )
        );
    }

    #[tokio::test]
    async fn list_tables_response() {
        let table_info = Page::new(
            vec![TableInfo::builder()
                .name("table_name")
                .schema_name("schema_name")
                .share_name("share_name")
                .id("table_id")
                .share_id("share_id")
                .storage_path("not important here")
                .build()
                .unwrap()],
            Some("page_token".to_owned()),
        );
        let response = ListTablesResponse::from(table_info);
        let res = response.into_response();

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers().get(CONTENT_TYPE).unwrap(),
            HeaderValue::from_static("application/json; charset=utf-8")
        );
        assert_eq!(
            to_bytes(res.into_body(), 1000).await.unwrap(),
            Bytes::from(
                r#"{"items":[{"name":"table_name","schema":"schema_name","share":"share_name","shareId":"share_id","id":"table_id"}],"nextPageToken":"page_token"}"#
            )
        );
    }

    #[tokio::test]
    async fn table_version_response() {
        let response = TableVersionResponse {
            version: TableVersionNumber::new(123),
        };
        let res = response.into_response();

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers().get("Delta-Table-Version").unwrap(),
            HeaderValue::from_static("123")
        );
    }
}
