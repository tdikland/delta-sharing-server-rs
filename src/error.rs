#![allow(missing_docs)]

use std::fmt::Display;

use axum::{http::header, http::StatusCode, response::IntoResponse, Json};
use serde::Serialize;

use crate::{
    catalog::{CatalogError, CatalogErrorKind},
    reader::TableReaderError,
};

pub type SharingServerResult<T> = core::result::Result<T, ServerError>;
pub type Result<T> = core::result::Result<T, ServerError>;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ServerErrorKind {
    InvalidParameters,
    Unauthorized,
    Forbidden,
    ResourceNotFound,
    Internal,
    UnsupportedOperation,
}

impl Display for ServerErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidParameters => write!(f, "INVALID_PARAMETERS"),
            Self::Unauthorized => write!(f, "UNAUTHORIZED"),
            Self::Forbidden => write!(f, "FORBIDDEN"),
            Self::ResourceNotFound => write!(f, "RESOURCE_NOT_FOUND"),
            Self::Internal => write!(f, "INTERNAL"),
            Self::UnsupportedOperation => write!(f, "UNSUPPORTED_OPERATION"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ServerError {
    kind: ServerErrorKind,
    message: String,
}

impl ServerError {
    pub fn new(kind: ServerErrorKind, message: String) -> Self {
        Self { kind, message }
    }

    pub fn kind(&self) -> ServerErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn invalid_query_params(message: impl Into<String>) -> Self {
        Self::new(ServerErrorKind::InvalidParameters, message.into())
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(ServerErrorKind::Unauthorized, message.into())
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::new(ServerErrorKind::Forbidden, message.into())
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ServerErrorKind::ResourceNotFound, message.into())
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ServerErrorKind::Internal, message.into())
    }

    pub fn unsupported_operation(message: impl Into<String>) -> Self {
        Self::new(ServerErrorKind::UnsupportedOperation, message.into())
    }

    pub fn into_error_response(self) -> ErrorResponse {
        ErrorResponse {
            error_code: self.kind.to_string(),
            message: self.message,
        }
    }
}

impl Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.kind, self.message)
    }
}

impl std::error::Error for ServerError {}

impl From<CatalogError> for ServerError {
    fn from(err: CatalogError) -> Self {
        match err.kind() {
            CatalogErrorKind::ResourceNotFound => ServerError::not_found(err.message()),
            CatalogErrorKind::MalformedPagination => {
                ServerError::invalid_query_params(err.message())
            }
            CatalogErrorKind::Internal => ServerError::internal(err.message()),
        }
    }
}

impl From<TableReaderError> for ServerError {
    fn from(err: TableReaderError) -> Self {
        ServerError::internal(err.to_string())
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorResponse {
    error_code: String,
    message: String,
}

impl IntoResponse for ServerError {
    fn into_response(self) -> axum::response::Response {
        tracing::error!(error = %self, details=?self, "Returning error response");
        let status_code = match self.kind() {
            ServerErrorKind::InvalidParameters => StatusCode::BAD_REQUEST,
            ServerErrorKind::Unauthorized => StatusCode::UNAUTHORIZED,
            ServerErrorKind::Forbidden => StatusCode::FORBIDDEN,
            ServerErrorKind::ResourceNotFound => StatusCode::NOT_FOUND,
            ServerErrorKind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
            ServerErrorKind::UnsupportedOperation => StatusCode::NOT_IMPLEMENTED,
        };

        (
            status_code,
            [(
                header::CONTENT_TYPE.as_str(),
                "application/json; charset=utf-8",
            )],
            Json(self.into_error_response()),
        )
            .into_response()
    }
}
