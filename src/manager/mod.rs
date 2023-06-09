#![warn(missing_docs)]

//! Types and traits for managing shared objects.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{error::Error, fmt::Display};

pub mod dynamo;
// pub mod mysql;
// pub mod postgres;

use crate::protocol::{
    securable::{Schema, Share, Table},
    share::{List, ListCursor},
};

/// Trait implemented by Share managers that each represent a different backing
/// store for the shared objects.
#[mockall::automock]
#[async_trait]
pub trait TableManager: Send + Sync {
    /// Fetch a list of shares stored on the sharing server store. The list
    /// cursor is used to limit the amount of returned shares and to resume
    /// listing from a specified point in the collection.
    async fn list_shares(&self, cursor: &ListCursor) -> Result<List<Share>, TableManagerError>;

    /// Get share details by name
    async fn get_share(&self, share_name: &str) -> Result<Share, TableManagerError>;

    /// Fetch a list of schemas stored on the sharing server store under a
    /// spcific share. The list cursor is used to limit the amount of returned
    /// schemas and to resume listing from a specified point in the collection.
    async fn list_schemas(
        &self,
        share_name: &str,
        cursor: &ListCursor,
    ) -> Result<List<Schema>, TableManagerError>;

    /// Fetch a list of tables stored on the sharing server store under a
    /// spcific share combination. The list cursor is used to limit
    /// the amount of returned tables and to resume listing from a specified
    /// point in the collection.
    async fn list_tables_in_share(
        &self,
        share_name: &str,
        cursor: &ListCursor,
    ) -> Result<List<Table>, TableManagerError>;

    /// Fetch a list of tables stored on the sharing server store under a
    /// spcific share + schema combination. The list cursor is used to limit
    /// the amount of returned tables and to resume listing from a specified
    /// point in the collection.
    async fn list_tables_in_schema(
        &self,
        share_name: &str,
        schema_name: &str,
        cursor: &ListCursor,
    ) -> Result<List<Table>, TableManagerError>;

    /// Get table specifics for a combination of share + schema + name.
    async fn get_table(
        &self,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Table, TableManagerError>;
}

/// Errors that can occur during the listing and retrieval of shared objects.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TableManagerError {
    /// The requested share was not found in the backing store.
    ShareNotFound {
        /// The name of the share that could not be found.
        share_name: String,
    },
    /// The requested schema was not found in the backing store.
    SchemaNotFound {
        /// The name of the share where the schema was searched.
        share_name: String,
        /// The name of the schema that could not be found.
        schema_name: String,
    },
    /// The requested table was not found in the backing store.
    TableNotFound {
        /// The name of the share where the table was searched.
        share_name: String,
        /// The name of the schema where the table was searched.
        schema_name: String,
        /// The name of the table that could not be found.
        table_name: String,
    },
    /// The token in the list cursor could not be serialized.
    MalformedContinuationToken,
    /// The connection to the backing store could not be established.
    ConnectionError,
    /// Other error
    Other {
        /// Reason why this error occurred.
        reason: String,
    },
}

impl Display for TableManagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableManagerError::ShareNotFound { share_name } => {
                write!(f, "share `{}` could not be found", share_name)
            }
            TableManagerError::SchemaNotFound {
                share_name,
                schema_name,
            } => write!(
                f,
                "schema `{}.{}` could not be found",
                share_name, schema_name
            ),
            TableManagerError::TableNotFound {
                share_name,
                schema_name,
                table_name,
            } => write!(
                f,
                "table `{}.{}.{}` could not be found",
                share_name, schema_name, table_name
            ),
            TableManagerError::MalformedContinuationToken => {
                write!(f, "the provided `page_token` is malformed")
            }
            TableManagerError::ConnectionError => {
                write!(f, "could not connect with the share manager")
            }
            TableManagerError::Other { .. } => write!(f, "another error occurred"),
        }
    }
}

impl Error for TableManagerError {}
