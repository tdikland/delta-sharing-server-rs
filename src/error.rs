#![allow(missing_docs)]

use axum::{http::header, http::StatusCode, response::IntoResponse, Json};
use serde::Serialize;

use crate::{manager::ShareIoError, reader::TableReaderError};

#[derive(Debug, Clone, PartialEq)]
pub enum ServerError {
    // input validation errors
    InvalidPaginationParameters { reason: String },
    InvalidTableVersion,
    InvalidTableDataPredicates,
    InvalidTableChangePredicates,
    InvalidTableStartingTimestamp,
    InvalidTableVersionRange { reason: String },
    // share IO errors
    InvalidPaginationToken { reason: String },
    ShareNotFound { name: String },
    SchemaNotFound { name: String },
    TableNotFound { name: String },
    ShareManagerError { reason: String },
    // table IO errors
    TableReaderError { reason: String },
    // sharing configuration errors
    UnsupportedTableFormat { format: String },
    UnsupportedTableStorage { storage: String },
    UnsupportedOperation { reason: String },
}

impl ServerError {
    pub fn into_error_response(self) -> ErrorResponse {
        match self {
            ServerError::InvalidPaginationParameters { .. } => ErrorResponse {
                error_code: String::from("INVALID_PARAMETER_VALUE"),
                message: String::from("the `pageToken` or `maxResults` parameter is invalid"),
            },
            ServerError::InvalidPaginationToken { .. } => ErrorResponse {
                error_code: String::from("INVALID_PARAMETER_VALUE"),
                message: String::from("the `pageToken` query parameter is invalid"),
            },
            ServerError::ShareNotFound { name } => ErrorResponse {
                error_code: String::from("RESOURCE_DOES_NOT_EXIST"),
                message: format!("share `{}` not found", name),
            },
            ServerError::SchemaNotFound { name } => ErrorResponse {
                error_code: String::from("RESOURCE_DOES_NOT_EXIST"),
                message: format!("schema `{}` not found", name),
            },
            ServerError::TableNotFound { name } => ErrorResponse {
                error_code: String::from("RESOURCE_DOES_NOT_EXIST"),
                message: format!("table `{}` not found", name),
            },
            ServerError::ShareManagerError { .. } => ErrorResponse {
                error_code: String::from("INTERNAL_ERROR"),
                message: String::new(),
            },

            _ => ErrorResponse {
                error_code: String::from("Something went wrong"),
                message: String::from("check your code"),
            },
        }
    }
}

pub type Result<T> = core::result::Result<T, ServerError>;

impl From<ShareIoError> for ServerError {
    fn from(value: ShareIoError) -> Self {
        match value {
            ShareIoError::MalformedContinuationToken => ServerError::InvalidPaginationToken {
                reason: value.to_string(),
            },
            ShareIoError::ShareNotFound { share_name } => {
                ServerError::ShareNotFound { name: share_name }
            }
            ShareIoError::SchemaNotFound {
                share_name,
                schema_name,
            } => ServerError::TableNotFound {
                name: format!("{}.{}", share_name, schema_name),
            },
            ShareIoError::TableNotFound {
                share_name,
                schema_name,
                table_name,
            } => Self::TableNotFound {
                name: format!("{}.{}.{}", share_name, schema_name, table_name),
            },
            ShareIoError::ConnectionError => ServerError::ShareManagerError {
                reason: String::new(),
            },
            ShareIoError::Other { reason } => ServerError::ShareManagerError { reason },
        }
    }
}

impl From<TableReaderError> for ServerError {
    fn from(value: TableReaderError) -> Self {
        ServerError::TableReaderError {
            reason: value.to_string(),
        }
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
        match self {
            Self::ShareNotFound { .. } => (
                StatusCode::NOT_FOUND,
                [(
                    header::CONTENT_TYPE.as_str(),
                    "application/json; charset=utf-8",
                )],
                Json(self.into_error_response()),
            )
                .into_response(),
            Self::TableNotFound { .. } => (
                StatusCode::NOT_FOUND,
                [(
                    header::CONTENT_TYPE.as_str(),
                    "application/json; charset=utf-8",
                )],
                Json(self.into_error_response()),
            )
                .into_response(),
            _ => (
                StatusCode::BAD_REQUEST,
                [(
                    header::CONTENT_TYPE.as_str(),
                    "application/json; charset=utf-8",
                )],
                Json(self.into_error_response()),
            )
                .into_response(),
        }
    }
}
