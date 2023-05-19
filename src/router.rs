use std::sync::Arc;

use axum::{
    routing::{get, post},
    Router,
};

use crate::{handler, state::SharingServerState};

pub fn get_router(state: Arc<SharingServerState>) -> Router {
    Router::new()
        .route("/shares", get(handler::list_shares))
        .route("/shares/:share", get(handler::get_share))
        .route("/shares/:share/schemas", get(handler::list_schemas))
        .route(
            "/shares/:share/schemas/:schema/tables",
            get(handler::list_tables_in_schema),
        )
        .route(
            "/shares/:share/all-tables",
            get(handler::list_tables_in_share),
        )
        .route(
            "/shares/:share/schemas/:schema/tables/:table/version",
            get(handler::get_table_version),
        )
        .route(
            "/shares/:share/schemas/:schema/tables/:table/metadata",
            get(handler::get_table_metadata),
        )
        .route(
            "/shares/:share/schemas/:schema/tables/:table/query",
            post(handler::get_table_data),
        )
        .route(
            "/shares/:share/schemas/:schema/tables/:table/changes",
            get(handler::get_table_changes),
        )
        .with_state(state)
}
