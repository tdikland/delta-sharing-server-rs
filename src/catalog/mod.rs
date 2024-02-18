//! Types and traits for managing shared objects.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{error::Error, fmt::Display};

use crate::auth::ClientId;

pub mod ddb;
// pub mod dynamo;
pub mod file;
// pub mod mysql;
// pub mod postgres;

/// Trait implemented by Share managers that each represent a different backing
/// store for the shared objects.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait Catalog: Send + Sync {
    /// Fetch a list of shares stored on the sharing server store. The list
    /// cursor is used to limit the amount of returned shares and to resume
    /// listing from a specified point in the collection.
    async fn list_shares(
        &self,
        client_id: &ClientId,
        pagination: &Pagination,
    ) -> Result<Page<ShareInfo>, CatalogError>;

    /// Fetch a list of schemas stored on the sharing server store under a
    /// spcific share. The list cursor is used to limit the amount of returned
    /// schemas and to resume listing from a specified point in the collection.
    async fn list_schemas(
        &self,
        client_id: &ClientId,
        share_name: &str,
        cursor: &Pagination,
    ) -> Result<Page<SchemaInfo>, CatalogError>;

    /// Fetch a list of tables stored on the sharing server store under a
    /// spcific share combination. The list cursor is used to limit
    /// the amount of returned tables and to resume listing from a specified
    /// point in the collection.
    async fn list_tables_in_share(
        &self,
        client_id: &ClientId,
        share_name: &str,
        cursor: &Pagination,
    ) -> Result<Page<TableInfo>, CatalogError>;

    /// Fetch a list of tables stored on the sharing server store under a
    /// spcific share + schema combination. The list cursor is used to limit
    /// the amount of returned tables and to resume listing from a specified
    /// point in the collection.
    async fn list_tables_in_schema(
        &self,
        client_id: &ClientId,
        share_name: &str,
        schema_name: &str,
        cursor: &Pagination,
    ) -> Result<Page<TableInfo>, CatalogError>;

    /// Get share details by name
    async fn get_share(
        &self,
        client_id: &ClientId,
        share_name: &str,
    ) -> Result<ShareInfo, CatalogError>;

    /// Get table specifics for a combination of share + schema + name.
    async fn get_table(
        &self,
        client_id: &ClientId,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<TableInfo, CatalogError>;
}

/// Pagination parameters for listing shared objects.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Pagination {
    pub(crate) max_results: Option<u32>,
    pub(crate) page_token: Option<String>,
}

impl Pagination {
    /// Create a new pagination object with the specified maximum results and
    /// page token.
    pub fn new(max_results: Option<u32>, page_token: Option<String>) -> Self {
        Self {
            max_results,
            page_token,
        }
    }

    pub fn max_results(&self) -> Option<u32> {
        self.max_results
    }

    pub fn page_token(&self) -> Option<&str> {
        self.page_token.as_deref()
    }
}

/// A page of shared objects.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Page<T> {
    pub(crate) items: Vec<T>,
    pub(crate) next_page_token: Option<String>,
}

impl<T> Page<T> {
    pub fn new(items: Vec<T>, next_page_token: Option<String>) -> Self {
        Self {
            items,
            next_page_token,
        }
    }

    pub fn items(&self) -> &Vec<T> {
        &self.items
    }

    pub fn next_page_token(&self) -> Option<&str> {
        self.next_page_token.as_deref()
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn into_parts(self) -> (Vec<T>, Option<String>) {
        (self.items, self.next_page_token)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShareInfo {
    pub(crate) name: String,
    pub(crate) id: Option<String>,
}

impl ShareInfo {
    pub fn new(name: String, id: Option<String>) -> Self {
        Self { name, id }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaInfo {
    pub(crate) name: String,
    pub(crate) share_name: String,
}

impl SchemaInfo {
    pub fn new(name: String, share_name: String) -> Self {
        Self { name, share_name }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn share_name(&self) -> &str {
        &self.share_name
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableInfo {
    pub(crate) name: String,
    pub(crate) schema_name: String,
    pub(crate) share_name: String,
    pub(crate) storage_location: String,
    pub(crate) id: Option<String>,
    pub(crate) share_id: Option<String>,
}

impl TableInfo {
    pub fn new(
        name: String,
        schema_name: String,
        share_name: String,
        storage_location: String,
    ) -> Self {
        Self {
            name,
            schema_name,
            share_name,
            storage_location,
            id: None,
            share_id: None,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn schema_name(&self) -> &str {
        &self.schema_name
    }

    pub fn share_name(&self) -> &str {
        &self.share_name
    }

    pub fn storage_path(&self) -> &str {
        &self.storage_location
    }
}

/// Errors that can occur during the listing and retrieval of shared objects.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CatalogError {
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

impl CatalogError {
    pub fn share_not_found(share_name: impl Into<String>) -> Self {
        Self::ShareNotFound {
            share_name: share_name.into(),
        }
    }

    pub fn schema_not_found(share_name: impl Into<String>, schema_name: impl Into<String>) -> Self {
        Self::SchemaNotFound {
            share_name: share_name.into(),
            schema_name: schema_name.into(),
        }
    }

    pub fn table_not_found(
        share_name: impl Into<String>,
        schema_name: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Self {
        Self::TableNotFound {
            share_name: share_name.into(),
            schema_name: schema_name.into(),
            table_name: table_name.into(),
        }
    }

    pub fn malformed_pagination(_msg: impl Into<String>) -> Self {
        Self::MalformedContinuationToken
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self::Other { reason: msg.into() }
    }
}

impl Display for CatalogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogError::ShareNotFound { share_name } => {
                write!(f, "share `{}` could not be found", share_name)
            }
            CatalogError::SchemaNotFound {
                share_name,
                schema_name,
            } => write!(
                f,
                "schema `{}.{}` could not be found",
                share_name, schema_name
            ),
            CatalogError::TableNotFound {
                share_name,
                schema_name,
                table_name,
            } => write!(
                f,
                "table `{}.{}.{}` could not be found",
                share_name, schema_name, table_name
            ),
            CatalogError::MalformedContinuationToken => {
                write!(f, "the provided `page_token` is malformed")
            }
            CatalogError::ConnectionError => {
                write!(f, "could not connect with the share manager")
            }
            CatalogError::Other { .. } => write!(f, "another error occurred"),
        }
    }
}

impl Error for CatalogError {}
