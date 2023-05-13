use axum::{http::header, http::StatusCode, response::IntoResponse, Json};
use serde::Serialize;

use crate::{manager::TableManagerError, reader::TableReaderError};

#[derive(Debug, PartialEq)]
pub enum ServerError {
    InvalidPagination {
        reason: String,
    },
    InvalidTableStartingTimestamp,
    InvalidTableVersionRange {
        reason: String,
    },
    TableReaderNotImplemented,
    UrlSignerNotImplemented,
    ShareNotFound {
        name: String,
    },
    TableNotFound {
        share: String,
        schema: String,
        name: String,
    },
    Other,
}

impl ServerError {
    pub fn into_error_response(self) -> ErrorResponse {
        match self {
            ServerError::ShareNotFound { name } => ErrorResponse {
                error_code: String::from("RESOURCE_DOES_NOT_EXIST"),
                message: format!("share `{}` not found", name),
            },
            ServerError::TableNotFound {
                share,
                schema,
                name,
            } => ErrorResponse {
                error_code: String::from("RESOURCE_DOES_NOT_EXIST"),
                message: format!("table `{}.{}.{}` not found", share, schema, name),
            },
            ServerError::InvalidPagination { .. } => ErrorResponse {
                error_code: String::from("400"),
                message: String::from("Malformed pagination"),
            },
            _ => ErrorResponse {
                error_code: String::from("Something went wrong"),
                message: String::from("check your code"),
            },
        }
    }
}

pub type Result<T> = core::result::Result<T, ServerError>;

impl From<TableManagerError> for ServerError {
    fn from(value: TableManagerError) -> Self {
        match value {
            TableManagerError::TableNotFound {
                share_name,
                schema_name,
                table_name,
            } => Self::TableNotFound {
                share: share_name,
                schema: schema_name,
                name: table_name,
            },
            TableManagerError::ShareNotFound { share_name } => {
                Self::ShareNotFound { name: share_name }
            }
            TableManagerError::InvalidListCursor => ServerError::InvalidPagination {
                reason: String::from(""),
            },
            // TableManagerError::InternalError => Self::ShareStore,
            // TableManagerError::Other => Self::Other,
            // TableManagerError::MalformedListCursor => Self::InvalidPagination {
            //     reason: String::from("UNKNNWON"),
            // },
            _ => Self::Other,
        }
    }
}

impl From<TableReaderError> for ServerError {
    fn from(value: TableReaderError) -> Self {
        match value {
            _ => Self::Other,
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
