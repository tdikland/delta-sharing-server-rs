//! Handlers for the endpoints defined by the Delta Sharing protocol.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum_macros::debug_handler;

use crate::{
    error::{Result, ServerError},
    extract::{Pagination, TableChangePredicates, TableDataPredicates, TableVersion},
    protocol::table::Version,
    response::{
        GetShareResponse, ListSchemasResponse, ListSharesResponse, ListTablesResponse,
        TableActionsResponse, TableVersionResponse,
    },
    state::SharingServerState,
};

#[debug_handler]
pub async fn list_shares(
    state: State<Arc<SharingServerState>>,
    pagination: Pagination,
) -> Result<ListSharesResponse> {
    state.list_shares(&pagination).await
}

#[debug_handler]
pub async fn get_share(
    state: State<Arc<SharingServerState>>,
    share_name: Path<String>,
) -> Result<GetShareResponse> {
    state.get_share(&share_name).await
}

#[debug_handler]
pub async fn list_schemas(
    state: State<Arc<SharingServerState>>,
    share_name: Path<String>,
    pagination: Pagination,
) -> Result<ListSchemasResponse> {
    state.list_schemas(&share_name, &pagination).await
}

#[debug_handler]
pub async fn list_tables_in_share(
    state: State<Arc<SharingServerState>>,
    share_name: Path<String>,
    pagination: Pagination,
) -> Result<ListTablesResponse> {
    state.list_tables_in_share(&share_name, &pagination).await
}

#[debug_handler]
pub async fn list_tables_in_schema(
    state: State<Arc<SharingServerState>>,
    Path((share_name, schema_name)): Path<(String, String)>,
    pagination: Pagination,
) -> Result<ListTablesResponse> {
    state
        .list_tables_in_schema(&share_name, &schema_name, &pagination)
        .await
}

#[debug_handler]
pub async fn get_table_version(
    state: State<Arc<SharingServerState>>,
    Path((share_name, schema_name, table_name)): Path<(String, String, String)>,
    tv: TableVersion,
) -> Result<TableVersionResponse> {
    state
        .get_table_version(&share_name, &schema_name, &table_name, tv.into_version())
        .await
}

#[debug_handler]
pub async fn get_table_metadata(
    State(state): State<Arc<SharingServerState>>,
    Path((share_name, schema_name, table_name)): Path<(String, String, String)>,
) -> Result<TableActionsResponse> {
    state
        .get_table_metadata(&share_name, &schema_name, &table_name)
        .await
}

#[debug_handler]
pub async fn get_table_data(
    State(state): State<Arc<SharingServerState>>,
    Path((share_name, schema_name, table_name)): Path<(String, String, String)>,
    _predicates: TableDataPredicates,
) -> Result<TableActionsResponse> {
    state
        .get_table_data(&share_name, &schema_name, &table_name, Version::Latest)
        .await
}

#[debug_handler]
pub async fn get_table_changes(
    State(_state): State<Arc<SharingServerState>>,
    Path((_share_name, _schema_name, _table_name)): Path<(String, String, String)>,
    _version_range: TableChangePredicates,
) -> Result<TableActionsResponse> {
    Err(ServerError::UnsupportedOperation {
        reason: String::from("The `table_changes` endpoint is not yet implemented."),
    })
}
