//! Router for the sharing server.

use std::sync::Arc;

use axum::debug_handler;
use axum::{
    extract::{Path, State},
    routing::{get, post},
    Router,
};
use tracing::{info_span, Instrument};

use crate::extract::Capabilities;
use crate::{
    auth::RecipientId,
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
pub fn build_sharing_router(state: Arc<SharingServerState>) -> Router {
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
    recipient_id: RecipientId,
    pagination: Pagination,
) -> Result<ListSharesResponse> {
    let span = info_span!("list shares", ?recipient_id, ?pagination);
    state
        .list_shares(&recipient_id, &pagination)
        .instrument(span)
        .await
}

#[debug_handler]
async fn list_schemas(
    state: State<Arc<SharingServerState>>,
    recipient_id: RecipientId,
    share_name: Path<String>,
    pagination: Pagination,
) -> Result<ListSchemasResponse> {
    let span = info_span!("list schemas", ?recipient_id, ?pagination);
    state
        .list_schemas(&recipient_id, &share_name, &pagination)
        .instrument(span)
        .await
}

#[debug_handler]
async fn list_tables_in_share(
    state: State<Arc<SharingServerState>>,
    recipient_id: RecipientId,
    share_name: Path<String>,
    pagination: Pagination,
) -> Result<ListTablesResponse> {
    let span = info_span!(
        "list tables in share",
        ?recipient_id,
        ?share_name,
        ?pagination
    );
    state
        .list_tables_in_share(&recipient_id, &share_name, &pagination)
        .instrument(span)
        .await
}

#[debug_handler]
async fn list_tables_in_schema(
    state: State<Arc<SharingServerState>>,
    recipient_id: RecipientId,
    Path((share_name, schema_name)): Path<(String, String)>,
    pagination: Pagination,
) -> Result<ListTablesResponse> {
    let span = info_span!(
        "list tables in schema",
        ?recipient_id,
        ?share_name,
        ?schema_name,
        ?pagination
    );
    state
        .list_tables_in_schema(&recipient_id, &share_name, &schema_name, &pagination)
        .instrument(span)
        .await
}

#[debug_handler]
async fn get_share(
    state: State<Arc<SharingServerState>>,
    recipient_id: RecipientId,
    share_name: Path<String>,
) -> Result<GetShareResponse> {
    let span = info_span!("get share", ?recipient_id, ?share_name);
    state
        .get_share(&recipient_id, &share_name)
        .instrument(span)
        .await
}

#[debug_handler]
async fn get_table_version(
    state: State<Arc<SharingServerState>>,
    recipient_id: RecipientId,
    Path((share_name, schema_name, table_name)): Path<(String, String, String)>,
    version: Version,
) -> Result<TableVersionResponse> {
    let span = info_span!(
        "get table version",
        ?recipient_id,
        ?share_name,
        ?schema_name,
        ?table_name
    );
    state
        .get_table_version_number(
            &recipient_id,
            &share_name,
            &schema_name,
            &table_name,
            version,
        )
        .instrument(span)
        .await
}

#[debug_handler]
async fn get_table_metadata(
    state: State<Arc<SharingServerState>>,
    recipient_id: RecipientId,
    capabilities: Capabilities,
    Path((share_name, schema_name, table_name)): Path<(String, String, String)>,
) -> Result<TableActionsResponse> {
    let span = info_span!(
        "get table metadata",
        ?recipient_id,
        ?capabilities,
        ?share_name,
        ?schema_name,
        ?table_name
    );
    state
        .get_table_metadata(
            &recipient_id,
            &share_name,
            &schema_name,
            &table_name,
            &capabilities,
        )
        .instrument(span)
        .await
}

#[debug_handler]
async fn get_table_data(
    state: State<Arc<SharingServerState>>,
    recipient_id: RecipientId,
    capabilities: Capabilities,
    Path((share_name, schema_name, table_name)): Path<(String, String, String)>,
    // _predicates: TableDataPredicates,
) -> Result<TableActionsResponse> {
    let span = info_span!(
        "get table data",
        ?recipient_id,
        ?capabilities,
        ?share_name,
        ?schema_name,
        ?table_name
    );
    state
        .get_table_data(
            &recipient_id,
            &share_name,
            &schema_name,
            &table_name,
            Version::Latest,
            &capabilities,
        )
        .instrument(span)
        .await
}

#[debug_handler]
async fn get_table_changes(
    _state: State<Arc<SharingServerState>>,
    _recipient_id: RecipientId,
    _capabilities: Capabilities,
    Path((_share_name, _schema_name, _table_name)): Path<(String, String, String)>,
    // _version_range: TableChangePredicates,
) -> Result<TableActionsResponse> {
    Err(ServerError::unsupported_operation(
        "table changes support not yet implemented",
    ))
}
