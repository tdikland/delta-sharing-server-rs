use std::{ops::Deref, sync::Arc};

use axum::extract::{Path, State};
use axum_macros::debug_handler;

use crate::{
    error::{Result, ServerError},
    extract::{Pagination, TableChangePredicates, TableDataPredicates, TableVersion},
    reader::Version,
    response::{
        GetShareResponse, ListSchemasResponse, ListSharesResponse, ListTablesResponse,
        TableInfoResponse, TableVersionResponse,
    },
    state::ShareApiState,
};

#[debug_handler]
pub async fn list_shares(
    state: State<Arc<ShareApiState>>,
    pagination: Pagination,
) -> Result<ListSharesResponse> {
    let list_shares = state.table_manager().list_shares(&pagination).await?;
    let response = ListSharesResponse::from(list_shares);
    Ok(response)
}

#[debug_handler]
pub async fn get_share(
    state: State<Arc<ShareApiState>>,
    share_name: Path<String>,
) -> Result<GetShareResponse> {
    let get_share = state.table_manager().get_share(&share_name).await?;
    let response = GetShareResponse::from(get_share);
    Ok(response)
}

#[debug_handler]
pub async fn list_schemas(
    state: State<Arc<ShareApiState>>,
    share_name: Path<String>,
    pagination: Pagination,
) -> Result<ListSchemasResponse> {
    let list_schemas = state
        .table_manager()
        .list_schemas(&share_name, &pagination)
        .await?;
    let response = ListSchemasResponse::from(list_schemas);
    Ok(response)
}

#[debug_handler]
pub async fn list_tables_in_share(
    state: State<Arc<ShareApiState>>,
    share_name: Path<String>,
    pagination: Pagination,
) -> Result<ListTablesResponse> {
    let list_tables = state
        .table_manager()
        .list_tables_in_share(&share_name, &pagination)
        .await?;
    let response = ListTablesResponse::from(list_tables);
    Ok(response)
}

#[debug_handler]
pub async fn list_tables_in_schema(
    state: State<Arc<ShareApiState>>,
    Path((share_name, schema_name)): Path<(String, String)>,
    pagination: Pagination,
) -> Result<ListTablesResponse> {
    let list_tables = state
        .table_manager()
        .list_tables_in_schema(&share_name, &schema_name, &pagination)
        .await?;
    let response = ListTablesResponse::from(list_tables);
    Ok(response)
}

#[debug_handler]
pub async fn get_table_version(
    state: State<Arc<ShareApiState>>,
    Path((share_name, schema_name, table_name)): Path<(String, String, String)>,
    version: TableVersion,
) -> Result<TableVersionResponse> {
    let table = state
        .table_manager()
        .get_table(&share_name, &schema_name, &table_name)
        .await?;

    let version = match version {
        TableVersion::Timestamp(ts) => Version::Timestamp(ts),
        TableVersion::Latest => Version::Latest,
    };

    let table_version = state
        .table_reader("DELTA")
        .ok_or(ServerError::TableReaderNotImplemented)?
        .get_table_version(table.storage_path(), version)
        .await?;

    let response = TableVersionResponse::from(table_version);
    Ok(response)
}

#[debug_handler]
pub async fn get_table_metadata(
    State(state): State<Arc<ShareApiState>>,
    Path((share_name, schema_name, table_name)): Path<(String, String, String)>,
) -> Result<TableInfoResponse> {
    let table = state
        .table_manager()
        .get_table(&share_name, &schema_name, &table_name)
        .await?;

    let table_metadata = state
        .table_reader("DELTA")
        .ok_or(ServerError::TableReaderNotImplemented)?
        .get_table_metadata(table.storage_path())
        .await?;

    let response = TableInfoResponse::from(table_metadata);
    Ok(response)
}

#[debug_handler]
pub async fn get_table_data(
    State(state): State<Arc<ShareApiState>>,
    Path((share_name, schema_name, table_name)): Path<(String, String, String)>,
    _predicates: TableDataPredicates,
) -> Result<TableInfoResponse> {
    let table = state
        .table_manager()
        .get_table(&share_name, &schema_name, &table_name)
        .await?;

    let table_data = state
        .table_reader(table.table_format().unwrap_or(&String::from("DELTA")))
        .ok_or(ServerError::TableReaderNotImplemented)?
        .get_table_data(table.storage_path(), 0, 0, "")
        .await?;

    let signer = state
        .url_signer("S3")
        .ok_or(ServerError::UrlSignerNotImplemented)?;
    let signed_table_data = table_data.sign(signer.deref()).await;

    let response = TableInfoResponse::from(signed_table_data);
    Ok(response)
}

#[debug_handler]
pub async fn get_table_changes(
    State(_state): State<Arc<ShareApiState>>,
    Path((_share_name, _schema_name, _table_name)): Path<(String, String, String)>,
    _version_range: TableChangePredicates,
) -> Result<TableInfoResponse> {
    return Err(ServerError::Other);
}
