use async_trait::async_trait;
use base64::{engine::general_purpose, Engine as _};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::ops::Deref;

pub mod dynamo;

pub use dynamo::{DynamoConfig, DynamoTableManager};

use crate::protocol::shared::{Schema, Share, Table};

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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TableManagerError {
    ShareNotFound {
        name: String,
    },
    TableNotFound {
        share_name: String,
        schema_name: String,
        table_name: String,
    },
    MalformedListCursor,
    InternalError,
    Other,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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

    pub fn next_page_token(&self) -> Option<&str> {
        self.page_token.as_deref()
    }

    pub fn has_next_page_token(&self) -> bool {
        self.page_token.is_some()
    }

    pub fn from_cursor<T: Serialize>(cursor: &T) -> Result<String, TableManagerError> {
        let value =
            serde_json::to_vec(cursor).map_err(|_| TableManagerError::MalformedListCursor)?;
        Ok(general_purpose::URL_SAFE.encode(value))
    }

    pub fn to_cursor<T: DeserializeOwned>(&self) -> Result<Option<T>, TableManagerError> {
        if let Some(token) = &self.page_token {
            let value = general_purpose::URL_SAFE
                .decode(token)
                .map_err(|_| TableManagerError::MalformedListCursor)?;
            Ok(Some(serde_json::from_slice::<T>(&value).unwrap()))
        } else {
            Ok(None)
        }
    }
}

impl Default for ListCursor {
    fn default() -> Self {
        Self {
            max_results: Default::default(),
            page_token: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
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
