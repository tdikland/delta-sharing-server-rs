use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{error::Error, fmt::Display, ops::Deref};

pub mod dynamo;
pub mod mysql;
pub mod postgres;

use crate::protocol::securables::{Schema, Share, Table};

#[mockall::automock]
#[async_trait]
pub trait TableManager: Send + Sync {
    async fn list_shares(&self, cursor: &ListCursor) -> Result<List<Share>, TableManagerError>;

    async fn get_share(&self, share_name: &str) -> Result<Share, TableManagerError>;

    async fn list_schemas(
        &self,
        share_name: &str,
        cursor: &ListCursor,
    ) -> Result<List<Schema>, TableManagerError>;

    async fn list_tables_in_share(
        &self,
        share_name: &str,
        cursor: &ListCursor,
    ) -> Result<List<Table>, TableManagerError>;

    async fn list_tables_in_schema(
        &self,
        share_name: &str,
        schema_name: &str,
        cursor: &ListCursor,
    ) -> Result<List<Table>, TableManagerError>;

    async fn get_table(
        &self,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Table, TableManagerError>;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TableManagerError {
    ShareNotFound {
        share_name: String,
    },
    SchemaNotFound {
        share_name: String,
        schema_name: String,
    },
    TableNotFound {
        share_name: String,
        schema_name: String,
        table_name: String,
    },
    MalformedContinuationToken,
    ConnectionError,
    Other {
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
            TableManagerError::ConnectionError => todo!(),
            TableManagerError::Other { .. } => todo!(),
        }
    }
}

impl Error for TableManagerError {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListCursor {
    max_results: Option<u32>,
    page_token: Option<String>,
}

impl ListCursor {
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

    pub fn has_page_token(&self) -> bool {
        self.page_token.is_some()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
pub struct List<T> {
    items: Vec<T>,
    next_page_token: Option<String>,
}

impl<T> List<T> {
    pub fn new(items: Vec<T>, next_page_token: Option<String>) -> Self {
        Self {
            items,
            next_page_token,
        }
    }

    pub fn push(&mut self, item: T) {
        self.items.push(item);
    }

    pub fn items(&self) -> &[T] {
        self.items.as_ref()
    }

    pub fn next_page_token(&self) -> Option<&String> {
        self.next_page_token.as_ref()
    }
}

impl<T> Deref for List<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.items
    }
}
