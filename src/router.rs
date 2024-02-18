//! Router for the sharing server.

use std::sync::Arc;

use axum::debug_handler;
use axum::{
    extract::{Path, State},
    routing::{get, post},
    Extension, Router,
};

use crate::extract::Capabilities;
use crate::{
    auth::ClientId,
    catalog::Pagination,
    error::{Result, ServerError},
    reader::Version,
    response::{
        GetShareResponse, ListSchemasResponse, ListSharesResponse, ListTablesResponse,
        TableActionsResponse, TableVersionResponse,
    },
    state::SharingServerState,
};

/// Builds the router for the sharing server
pub fn build_sharing_server_router(state: Arc<SharingServerState>) -> Router {
    Router::new()
        .route("/shares", get(list_shares))
        .route("/shares/:share", get(get_share))
        .route("/shares/:share/schemas", get(list_schemas))
        .route(
            "/shares/:share/schemas/:schema/tables",
            get(list_tables_in_schema),
        )
        .route("/shares/:share/all-tables", get(list_tables_in_share))
        .route(
            "/shares/:share/schemas/:schema/tables/:table/version",
            get(get_table_version),
        )
        .route(
            "/shares/:share/schemas/:schema/tables/:table/metadata",
            get(get_table_metadata),
        )
        .route(
            "/shares/:share/schemas/:schema/tables/:table/query",
            post(get_table_data),
        )
        .route(
            "/shares/:share/schemas/:schema/tables/:table/changes",
            get(get_table_changes),
        )
        .with_state(state)
}

#[debug_handler]
async fn list_shares(
    state: State<Arc<SharingServerState>>,
    client_id: Extension<ClientId>,
    pagination: Pagination,
) -> Result<ListSharesResponse> {
    let share_info_page = state.list_shares(&client_id, &pagination).await?;
    Ok(ListSharesResponse::from(share_info_page))
}

#[debug_handler]
async fn list_schemas(
    state: State<Arc<SharingServerState>>,
    client_id: Extension<ClientId>,
    share_name: Path<String>,
    pagination: Pagination,
) -> Result<ListSchemasResponse> {
    let schema_info_page = state
        .list_schemas(&client_id, &share_name, &pagination)
        .await?;
    Ok(ListSchemasResponse::from(schema_info_page))
}

#[debug_handler]
async fn list_tables_in_share(
    state: State<Arc<SharingServerState>>,
    client_id: Extension<ClientId>,
    share_name: Path<String>,
    pagination: Pagination,
) -> Result<ListTablesResponse> {
    let table_info_page = state
        .list_tables_in_share(&client_id, &share_name, &pagination)
        .await?;
    Ok(ListTablesResponse::from(table_info_page))
}

#[debug_handler]
async fn list_tables_in_schema(
    state: State<Arc<SharingServerState>>,
    client_id: Extension<ClientId>,
    Path((share_name, schema_name)): Path<(String, String)>,
    pagination: Pagination,
) -> Result<ListTablesResponse> {
    let table_info_page = state
        .list_tables_in_schema(&client_id, &share_name, &schema_name, &pagination)
        .await?;
    Ok(ListTablesResponse::from(table_info_page))
}

#[debug_handler]
async fn get_share(
    state: State<Arc<SharingServerState>>,
    client_id: Extension<ClientId>,
    share_name: Path<String>,
) -> Result<GetShareResponse> {
    let share = state.catalog().get_share(&client_id, &share_name).await?;
    Ok(share.into())
}

#[debug_handler]
async fn get_table_version(
    state: State<Arc<SharingServerState>>,
    client_id: Extension<ClientId>,
    Path((share_name, schema_name, table_name)): Path<(String, String, String)>,
    version: Version,
) -> Result<TableVersionResponse> {
    let table_version_number = state
        .get_table_version_number(&client_id, &share_name, &schema_name, &table_name, version)
        .await?;
    Ok(TableVersionResponse::from(table_version_number))
}

#[debug_handler]
async fn get_table_metadata(
    state: State<Arc<SharingServerState>>,
    client_id: Extension<ClientId>,
    capabilities: Capabilities,
    Path((share_name, schema_name, table_name)): Path<(String, String, String)>,
) -> Result<TableActionsResponse> {
    let table_metadata = state
        .get_table_metadata(&client_id, &share_name, &schema_name, &table_name)
        .await?;

    // TODO: find out consequences of the delta-capability-header
    // e.g. check the protocol to see what reader features are required and checking against the header
    if capabilities.is_delta_format() && table_metadata.protocol.min_reader_version() > 1 {
        return Err(ServerError::UnsupportedOperation {
            reason: String::from("The required delta reader feature is not implemented."),
        });
    }

    Ok(TableActionsResponse::from(table_metadata))
}

#[debug_handler]
async fn get_table_data(
    state: State<Arc<SharingServerState>>,
    client_id: Extension<ClientId>,
    _capabilities: Capabilities,
    Path((share_name, schema_name, table_name)): Path<(String, String, String)>,
    // _predicates: TableDataPredicates,
) -> Result<TableActionsResponse> {
    // TODO: Get P&M before to asses if the client can read the table
    let data = state
        .get_table_data(
            &client_id,
            &share_name,
            &schema_name,
            &table_name,
            Version::Latest,
        )
        .await?;
    Ok(data.into())
}

#[debug_handler]
async fn get_table_changes(
    _state: State<Arc<SharingServerState>>,
    _client_id: Extension<ClientId>,
    _capabilities: Capabilities,
    Path((_share_name, _schema_name, _table_name)): Path<(String, String, String)>,
    // _version_range: TableChangePredicates,
) -> Result<TableActionsResponse> {
    Err(ServerError::UnsupportedOperation {
        reason: String::from("The `table_changes` endpoint is not yet implemented."),
    })
}
