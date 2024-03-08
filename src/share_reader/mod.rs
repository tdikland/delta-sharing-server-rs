//! Types and traits for managing shared objects.
//!
//! Every Delta Sharing server needs to know which shares, schemas and tables
//! are available to be shared with the client. The server also needs to know
//! where the specific tables are stored. This module provides the traits and
//! types that are used by the sharing server to list and get information about
//! the shared objects.
//!
//! The [`ShareReader`] trait is implemented by the different share managers
//! that each may represent a different backing store for the shared objects.
//! The [`ShareReader`] trait provides methods to list shares, schemas and
//! tables and to get details about a specific share or table. The
//! [`ClientId`]` type is used to identify the client that is requesting the
//! shared objects. Based on the passed [`ClientId`] the share manager can
//! decide which shares, schemas and tables are available to the client.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{error::Error, fmt::Display};

use crate::auth::ClientId;

pub mod dynamo;
// pub mod file;
pub mod postgres;

/// Trait for listing and reading shared objects in the Delta Sharing server.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait ShareReader: Send + Sync {
    /// Return a page of shares stored on the sharing server store accessible
    /// for the given client. The pagination argument is used to limit the
    /// amount of returned shares in this call and to resume listing from a
    /// specified point in the collection of shares.
    async fn list_shares(
        &self,
        client_id: &ClientId,
        pagination: &Pagination,
    ) -> Result<Page<Share>, ShareReaderError>;

    /// Return a page of schemas stored on the sharing server store that belong
    /// to the specified share and are accessible for the given client. The
    /// pagination argument is used to limit the amount of returned schemas in
    /// this call and to resume listing from a specified point in the
    /// collection of schemas.
    async fn list_schemas(
        &self,
        client_id: &ClientId,
        share_name: &str,
        cursor: &Pagination,
    ) -> Result<Page<Schema>, ShareReaderError>;

    /// Return a page of tables stored on the sharing server store that belong
    /// to the specified share and are accessible for the given client. The
    /// pagination argument is used to limit the amount of returned tables in
    /// this call and to resume listing from a specified point in the
    /// collection of tables.
    async fn list_tables_in_share(
        &self,
        client_id: &ClientId,
        share_name: &str,
        cursor: &Pagination,
    ) -> Result<Page<Table>, ShareReaderError>;

    /// Return a page of tables stored on the sharing server store that belong
    /// to the specified share+schema and are accessible for the given client.
    /// The pagination argument is used to limit the amount of returned tables
    /// in this call and to resume listing from a specified point in the
    /// collection of tables.
    async fn list_tables_in_schema(
        &self,
        client_id: &ClientId,
        share_name: &str,
        schema_name: &str,
        cursor: &Pagination,
    ) -> Result<Page<Table>, ShareReaderError>;

    /// Return a share with the specified name if it is accessible for the
    /// given client.
    async fn get_share(
        &self,
        client_id: &ClientId,
        share_name: &str,
    ) -> Result<Share, ShareReaderError>;

    /// Return a table with the specified name within the specified share and
    /// schema if it is accessible for the given client.
    async fn get_table(
        &self,
        client_id: &ClientId,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Table, ShareReaderError>;
}

/// Pagination parameters for listing shared objects.
#[derive(Debug, Clone, Default, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Pagination {
    max_results: Option<u32>,
    page_token: Option<String>,
}

impl Pagination {
    /// Create a new pagination object with the specified maximum results and
    /// page token.
    ///
    /// # Example
    /// ```rust
    /// use delta_sharing_server::share_reader::Pagination;
    ///
    /// let pagination = Pagination::new(Some(43), Some("foo".to_string()));
    /// assert_eq!(pagination.max_results(), Some(43));
    /// assert_eq!(pagination.page_token(), Some("foo"));
    /// ```
    pub fn new(max_results: Option<u32>, page_token: Option<String>) -> Self {
        Self {
            max_results,
            page_token,
        }
    }

    /// Return the maximum amount of results to be returned in a single page.
    ///
    /// # Example
    /// ```rust
    /// use delta_sharing_server::share_reader::Pagination;
    ///
    /// let pagination = Pagination::new(Some(43), None);
    /// assert_eq!(pagination.max_results(), Some(43));
    /// ```
    pub fn max_results(&self) -> Option<u32> {
        self.max_results
    }

    /// Return the token that can be used to resume listing from the specified
    /// point in the collection of shared objects.
    ///
    /// # Example
    /// ```rust
    /// use delta_sharing_server::share_reader::Pagination;
    ///
    /// let pagination = Pagination::new(None, Some("foo".to_string()));
    /// assert_eq!(pagination.page_token(), Some("foo"));
    /// ```
    pub fn page_token(&self) -> Option<&str> {
        self.page_token.as_deref()
    }
}

/// A page of shared objects returned from the [`ShareReader`].
#[derive(Debug, Clone, PartialEq, Hash, Serialize, Deserialize)]
pub struct Page<T> {
    items: Vec<T>,
    next_page_token: Option<String>,
}

impl<T> Page<T> {
    /// Create a new page with the specified items and token.
    ///
    /// # Example
    /// ```rust
    /// use delta_sharing_server::share_reader::{Page, Share};
    ///
    /// let shares = vec![
    ///     Share::new("foo".to_string(), None),
    ///     Share::new("bar".to_string(), None)
    /// ];
    /// let page = Page::new(shares, Some("token".to_string()));
    /// assert_eq!(page.len(), 2);
    /// assert_eq!(page.next_page_token(), Some("token"));
    /// ````
    pub fn new(items: Vec<T>, next_page_token: Option<String>) -> Self {
        Self {
            items,
            next_page_token,
        }
    }

    /// Return the shared objects in the page.
    ///
    /// # Example
    /// ```rust
    /// use delta_sharing_server::share_reader::{Page, Share};
    ///
    /// let shares = vec![
    ///     Share::new("foo".to_string(), None),
    ///     Share::new("bar".to_string(), None)
    /// ];
    /// let page = Page::new(shares.clone(), None);
    /// assert_eq!(page.items(), &shares);
    /// ````
    pub fn items(&self) -> &Vec<T> {
        &self.items
    }

    /// Return the token that can be used to resume listing from a specified
    /// point in the collection of shared objects.
    ///
    /// # Example
    /// ```rust
    /// use delta_sharing_server::share_reader::{Page, Share};
    ///
    /// let shares = vec![
    ///     Share::new("foo".to_string(), None),
    ///     Share::new("bar".to_string(), None)
    /// ];
    /// let page = Page::new(shares, Some("token".to_string()));
    /// assert_eq!(page.next_page_token(), Some("token"));
    /// ```
    pub fn next_page_token(&self) -> Option<&str> {
        self.next_page_token.as_deref()
    }

    /// Return the amount of shared objects in the page.
    ///
    /// # Example
    /// ```rust
    /// use delta_sharing_server::share_reader::{Page, Share};
    ///
    /// let shares = vec![
    ///     Share::new("foo".to_string(), None),
    ///     Share::new("bar".to_string(), None)
    /// ];
    /// let page = Page::new(shares, None);
    /// assert_eq!(page.len(), 2);
    /// ```
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Return whether the page is empty.
    ///
    /// # Example
    /// ```rust
    /// use delta_sharing_server::share_reader::{Page, Share};
    ///
    /// let page = Page::<Share>::new(vec![], None);
    /// assert!(page.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Return whether the page is the last page of the collection.
    ///
    /// If the next page token is `None`, then the page is the last page.
    ///
    /// # Example
    /// ```rust
    /// use delta_sharing_server::share_reader::{Page, Share};
    ///
    /// let page = Page::new(vec![Share::new("foo".to_string(), None)], None);
    /// assert!(page.is_last_page());
    /// ```
    pub fn is_last_page(&self) -> bool {
        self.next_page_token.is_none()
    }

    /// Convert the page into its parts: items and token.
    ///
    /// # Example
    /// ```rust
    /// use delta_sharing_server::share_reader::{Page, Share};
    ///
    /// let shares = vec![
    ///     Share::new("foo".to_string(), None),
    ///     Share::new("bar".to_string(), None)
    /// ];
    /// let page = Page::new(shares, Some("token".to_string()));
    /// let (items, token) = page.into_parts();
    /// assert_eq!(items, vec![
    ///     Share::new("foo".to_string(), None),
    ///     Share::new("bar".to_string(), None)
    /// ]);
    /// assert_eq!(token, Some("token".to_string()));
    /// ```
    pub fn into_parts(self) -> (Vec<T>, Option<String>) {
        (self.items, self.next_page_token)
    }
}

/// Information about a share stored on the sharing server store.
#[derive(Debug, Clone, PartialEq, Hash, Serialize, Deserialize)]
pub struct Share {
    id: Option<String>,
    name: String,
}

impl Share {
    /// Create a new share info with the specified name and id.
    pub fn new(name: String, id: Option<String>) -> Self {
        Self { name, id }
    }

    /// Create a new [`ShareBuilder`]
    pub fn builder() -> ShareBuilder {
        ShareBuilder::new()
    }

    /// Return the id of the share.
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    /// Return the name of the share.
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// A builder for the [`Share`] type
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct ShareBuilder {
    id: Option<String>,
    name: Option<String>,
}

impl ShareBuilder {
    /// Create a new [`ShareBuilder`]
    pub fn new() -> Self {
        Self {
            id: None,
            name: None,
        }
    }

    /// Set the id of the share
    pub fn id(mut self, share_id: impl Into<String>) -> Self {
        self.id = Some(share_id.into());
        self
    }

    /// Set the id of the share
    pub fn set_id(mut self, share_id: Option<String>) -> Self {
        self.id = share_id;
        self
    }

    /// Set the name of the share
    pub fn name(mut self, share_name: impl Into<String>) -> Self {
        self.name = Some(share_name.into());
        self
    }

    /// Set the name of the share
    pub fn set_name(mut self, share_name: Option<String>) -> Self {
        self.name = share_name;
        self
    }

    /// Build the share
    pub fn build(self) -> Result<Share, ShareReaderError> {
        let Some(share_name) = self.name else {
            return Err(ShareReaderError::internal(
                "the required attribute `name` was not set",
            ));
        };

        Ok(Share {
            id: self.id,
            name: share_name,
        })
    }
}

/// Information about a schema stored on the sharing server store.
#[derive(Debug, Clone, PartialEq, Hash, Serialize, Deserialize)]
pub struct Schema {
    id: Option<String>,
    name: String,
    share_name: String,
}

impl Schema {
    /// Create a new schema info with the specified name and share name.
    pub fn new(name: String, share_name: String) -> Self {
        Self {
            id: None,
            name,
            share_name,
        }
    }

    /// Create a new [`SchemaBuilder`]
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder::new()
    }

    /// Create a new schema info with the specified id, name and share name.
    pub fn new_with_id(id: String, name: String, share_name: String) -> Self {
        Self {
            id: Some(id),
            name,
            share_name,
        }
    }

    /// Return the id of the schema.
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    /// Return the name of the schema.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the name of the share where the schema is stored.
    pub fn share_name(&self) -> &str {
        &self.share_name
    }
}

/// A builder for the [`Schema`] type
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SchemaBuilder {
    schema_id: Option<String>,
    share_id: Option<String>,
    schema_name: Option<String>,
    share_name: Option<String>,
}

impl SchemaBuilder {
    /// Create a new [`SchemaBuilder`]
    pub fn new() -> Self {
        Default::default()
    }

    /// Set the id of the schema
    pub fn id(mut self, schema_id: impl Into<String>) -> Self {
        self.schema_id = Some(schema_id.into());
        self
    }

    /// Set the id of the schema
    pub fn set_id(mut self, schema_id: Option<String>) -> Self {
        self.schema_id = schema_id;
        self
    }

    /// Set the share_id of the share containing the schema
    pub fn share_id(mut self, share_id: impl Into<String>) -> Self {
        self.share_id = Some(share_id.into());
        self
    }

    /// Set the share_id of the share containing the schema
    pub fn set_share_id(mut self, share_id: Option<String>) -> Self {
        self.share_id = share_id;
        self
    }

    /// Set the name of the schema
    pub fn name(mut self, schema_name: impl Into<String>) -> Self {
        self.schema_name = Some(schema_name.into());
        self
    }

    /// Set the name of the schema
    pub fn set_name(mut self, schema_name: Option<String>) -> Self {
        self.schema_name = schema_name;
        self
    }

    /// Set the name of the share containing the schema
    pub fn share_name(mut self, share_name: impl Into<String>) -> Self {
        self.share_name = Some(share_name.into());
        self
    }

    /// Set the name of the share containing the schema
    pub fn set_share_name(mut self, share_name: Option<String>) -> Self {
        self.share_name = share_name;
        self
    }

    /// Build the schema
    pub fn build(self) -> Result<Schema, ShareReaderError> {
        let Some(schema_name) = self.schema_name else {
            return Err(ShareReaderError::internal(
                "the required attribute `name` was not set",
            ));
        };

        let Some(share_name) = self.share_name else {
            return Err(ShareReaderError::internal(
                "the required attribute `share_name` was not set",
            ));
        };

        Ok(Schema {
            id: self.schema_id,
            name: schema_name,
            share_name,
        })
    }
}

/// Information about a table stored on the sharing server store.
#[derive(Debug, Clone, PartialEq, Hash, Serialize, Deserialize)]
pub struct Table {
    id: Option<String>,
    name: String,
    share_id: Option<String>,
    share_name: String,
    schema_name: String,
    storage_location: String,
}

impl Table {
    /// Create a new table info with the specified name, schema name, share name
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

    /// Create a new [`TableBuilder`]
    pub fn builder() -> TableBuilder {
        TableBuilder::new()
    }

    /// Return the id of the table.
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    /// Return the id of the share where the table is stored.
    pub fn share_id(&self) -> Option<&str> {
        self.share_id.as_deref()
    }

    /// Return the name of the table.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the name of the schema where the table is stored.
    pub fn schema_name(&self) -> &str {
        &self.schema_name
    }

    /// Return the name of the share where the table is stored.
    pub fn share_name(&self) -> &str {
        &self.share_name
    }

    /// Return the storage location of the table.
    pub fn storage_path(&self) -> &str {
        &self.storage_location
    }
}

/// A builder for the [`Table`] type
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TableBuilder {
    share_id: Option<String>,
    schema_id: Option<String>,
    table_id: Option<String>,
    share_name: Option<String>,
    schema_name: Option<String>,
    table_name: Option<String>,
    storage_path: Option<String>,
}

impl TableBuilder {
    /// Create a new [`TableBuilder`]
    pub fn new() -> Self {
        Default::default()
    }

    /// Set the id of the share containing the table
    pub fn share_id(mut self, share_id: impl Into<String>) -> Self {
        self.share_id = Some(share_id.into());
        self
    }

    /// Set the id of the share containing the table
    pub fn set_share_id(mut self, share_id: Option<String>) -> Self {
        self.share_id = share_id;
        self
    }

    /// Set the id of the schema containing the table
    pub fn schema_id(mut self, schema_id: impl Into<String>) -> Self {
        self.schema_id = Some(schema_id.into());
        self
    }

    /// Set the id of the schema containing the table
    pub fn set_schema_id(mut self, schema_id: Option<String>) -> Self {
        self.schema_id = schema_id;
        self
    }

    /// Set the id of the table
    pub fn id(mut self, table_id: impl Into<String>) -> Self {
        self.table_id = Some(table_id.into());
        self
    }

    /// Set the id of the table
    pub fn set_id(mut self, table_id: Option<String>) -> Self {
        self.table_id = table_id;
        self
    }

    /// Set the name of the share containing the table
    pub fn share_name(mut self, share_name: impl Into<String>) -> Self {
        self.share_name = Some(share_name.into());
        self
    }

    /// Set the name of the share containing the table
    pub fn set_share_name(mut self, share_name: Option<String>) -> Self {
        self.share_name = share_name;
        self
    }

    /// Set the name of the schema containing the table
    pub fn schema_name(mut self, schema_name: impl Into<String>) -> Self {
        self.schema_name = Some(schema_name.into());
        self
    }

    /// Set the name of the schema containing the table
    pub fn set_schema_name(mut self, schema_name: Option<String>) -> Self {
        self.schema_name = schema_name;
        self
    }

    /// Set the name of the table
    pub fn name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = Some(table_name.into());
        self
    }

    /// Set the name of the table
    pub fn set_name(mut self, table_name: Option<String>) -> Self {
        self.table_name = table_name;
        self
    }

    /// Set the storage location of the table
    pub fn storage_path(mut self, path: impl Into<String>) -> Self {
        self.storage_path = Some(path.into());
        self
    }

    /// Set the storage location of the table
    pub fn set_storage_path(mut self, path: Option<String>) -> Self {
        self.storage_path = path;
        self
    }

    /// Build the table
    pub fn build(self) -> Result<Table, ShareReaderError> {
        let Some(share_name) = self.share_name else {
            return Err(ShareReaderError::internal(
                "the required attribute `share_name` was not set",
            ));
        };

        let Some(schema_name) = self.schema_name else {
            return Err(ShareReaderError::internal(
                "the required attribute `schema_name` was not set",
            ));
        };

        let Some(table_name) = self.table_name else {
            return Err(ShareReaderError::internal(
                "the required attribute `table_name` was not set",
            ));
        };

        let Some(storage_path) = self.storage_path else {
            return Err(ShareReaderError::internal(
                "the required attribute `storage_path` was not set",
            ));
        };

        Ok(Table {
            id: self.table_id,
            name: table_name,
            share_id: self.share_id,
            share_name,
            schema_name,
            storage_location: storage_path,
        })
    }
}

/// Errors that can occur during the listing and retrieval of shared objects.
#[derive(Debug, Clone, Copy, PartialEq, Hash, Serialize, Deserialize)]
pub enum ShareReaderErrorKind {
    /// The requested share or table was not found.
    ResourceNotFound,
    /// The pagination token is malformed.
    MalformedPagination,
    /// The [`ShareReader`] has an internal error.
    Internal,
}

/// Error that occurred during the listing and retrieval of shared objects.
///
/// This error is used to wrap the specific error that occurred and to provide
/// a message that can be used to describe the error.
#[derive(Debug, Clone, PartialEq, Hash, Serialize, Deserialize)]
pub struct ShareReaderError {
    kind: ShareReaderErrorKind,
    message: String,
}

impl ShareReaderError {
    /// Create a new error with the specified kind and message.
    pub fn new(kind: ShareReaderErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// Return the kind of the error.
    pub fn kind(&self) -> ShareReaderErrorKind {
        self.kind
    }

    /// Return the message of the error.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Create a new error indicating that the requested share or table was not
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ShareReaderErrorKind::ResourceNotFound, message)
    }

    /// Create a new error indicating that the pagination token is malformed.
    pub fn malformed_pagination(message: impl Into<String>) -> Self {
        Self::new(ShareReaderErrorKind::MalformedPagination, message)
    }

    /// Create a new error indicating that the [`ShareReader`] has an internal
    /// error.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ShareReaderErrorKind::Internal, message)
    }
}

impl Display for ShareReaderErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShareReaderErrorKind::ResourceNotFound => write!(f, "RESOURCE_NOT_FOUND"),
            ShareReaderErrorKind::MalformedPagination => write!(f, "MALFORMED_PAGINATION"),
            ShareReaderErrorKind::Internal => write!(f, "INTERNAL_ERROR"),
        }
    }
}

impl Display for ShareReaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.kind, self.message)
    }
}

impl Error for ShareReaderError {}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn share_builder() {
        let share = Share::builder()
            .id("id")
            .name("name")
            .build()
            .expect("failed to build share");

        assert_eq!(share.id(), Some("id"));
        assert_eq!(share.name(), "name");

        let share = Share::builder().id("id").build();
        assert!(share.is_err());
        assert_eq!(share.unwrap_err().kind(), ShareReaderErrorKind::Internal);
    }

    #[test]
    fn schema_builder() {
        let schema = Schema::builder()
            .id("id")
            .name("name")
            .share_name("share")
            .build()
            .expect("failed to build schema");

        assert_eq!(schema.id(), Some("id"));
        assert_eq!(schema.name(), "name");
        assert_eq!(schema.share_name(), "share");

        let schema = Schema::builder().id("id").build();
        assert!(schema.is_err());
        assert_eq!(schema.unwrap_err().kind(), ShareReaderErrorKind::Internal);
    }

    #[test]
    fn table_builder() {
        let table = Table::builder()
            .id("id")
            .name("name")
            .share_name("share")
            .schema_name("schema")
            .storage_path("path")
            .build()
            .expect("failed to build table");

        assert_eq!(table.id(), Some("id"));
        assert_eq!(table.share_id(), None);
        assert_eq!(table.name(), "name");
        assert_eq!(table.share_name(), "share");
        assert_eq!(table.schema_name(), "schema");
        assert_eq!(table.storage_path(), "path");

        let table = Table::builder().id("id").build();
        assert!(table.is_err());
        assert_eq!(table.unwrap_err().kind(), ShareReaderErrorKind::Internal);
    }
}
