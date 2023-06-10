//! Basic types for describing shared objects

use std::fmt::Display;

use serde::{Deserialize, Serialize};

/// The type of a share as defined in the Delta Sharing protocol.
///
/// A share is a logical grouping to share with recipients. A share can be
/// shared with one or multiple recipients. A recipient can access all
/// resources in a share. A share may contain multiple schemas.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd)]
pub struct Share {
    name: String,
    id: Option<String>,
}

/// The type of a schema as defined in the Delta Sharing protocol.
///
/// A schema is a logical grouping of tables. A schema may contain multiple
/// tables. A schema is defined within the context of a [`Share`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Hash)]
pub struct Schema {
    share: Share,
    name: String,
    id: Option<String>,
}

/// The type of a table as defined in the Delta Sharing protocol.
///
/// A table is a Delta Lake table or a view on top of a Delta Lake table. A
/// table is defined within the context of a [`Schema`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Hash)]
pub struct Table {
    schema: Schema,
    name: String,
    id: Option<String>,
    storage_path: String,
    format: String,
}

impl Share {
    /// Create a new `Share` with the given `name` and `id`.
    pub fn new<S: Into<String>>(name: S, id: Option<S>) -> Self {
        Self {
            name: name.into(),
            id: id.map(Into::into),
        }
    }

    /// Retrieve the name from `self`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use delta_sharing_server::protocol::securable::Share;
    ///
    /// let share = Share::new("my-share", None);
    /// assert_eq!(share.name(), "my-share");
    /// ```
    pub fn name(&self) -> &str {
        self.name.as_ref()
    }

    /// Retrieve the id from `self`.
    ///
    /// # Example
    ///  
    /// ```rust
    /// use delta_sharing_server::protocol::securable::Share;
    ///
    /// let share = Share::new("my-share", Some("my-share-id"));
    /// assert_eq!(share.id(), Some("my-share-id"));
    /// ```
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }
}

impl Display for Share {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl Schema {
    /// Create a new `Schema` with the given [`Share`], `name` and `id`.
    pub fn new<S: Into<String>>(share: Share, name: S, id: Option<S>) -> Self {
        Self {
            share,
            name: name.into(),
            id: id.map(Into::into),
        }
    }

    /// Returns the name of the share associated with `self`
    ///
    /// # Example
    ///
    /// ```rust
    /// use delta_sharing_server::protocol::securable::{Share, Schema};
    ///
    /// let share = Share::new("my-share", None);
    /// let schema = Schema::new(share, "my-schema", None);
    /// assert_eq!(schema.share_name(), "my-share");
    /// ```
    pub fn share_name(&self) -> &str {
        self.share.name()
    }

    /// Returns the id of the share associated with `self`
    ///
    /// # Example
    ///
    /// ```rust
    /// use delta_sharing_server::protocol::securable::{Share, Schema};
    ///
    /// let share = Share::new("my-share", Some("my-share-id"));
    /// let schema = Schema::new(share, "my-schema", None);
    /// assert_eq!(schema.share_id(), Some("my-share-id"));
    /// ```
    pub fn share_id(&self) -> Option<&str> {
        self.share.id()
    }

    /// Returns the name of `self`
    ///
    /// # Example
    ///
    /// ```rust
    /// use delta_sharing_server::protocol::securable::{Share, Schema};
    ///
    /// let share = Share::new("my-share", None);
    /// let schema = Schema::new(share, "my-schema", None);
    /// assert_eq!(schema.name(), "my-schema");
    /// ```
    pub fn name(&self) -> &str {
        self.name.as_ref()
    }

    /// Returns the id of `self`
    ///
    /// # Example
    ///
    /// ```rust
    /// use delta_sharing_server::protocol::securable::{Share, Schema};
    ///
    /// let share = Share::new("my-share", None);
    /// let schema = Schema::new(share, "my-schema", Some("my-schema-id"));
    /// assert_eq!(schema.id(), Some("my-schema-id"));
    /// ```
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }
}

impl Display for Schema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.share_name(), self.name())
    }
}

impl Table {
    /// Create a new `Table` with the given [`Schema`], `name`, `storage_path`,
    ///  `table_id` and `table_format`. Whenever the `table_id` is `None`, it
    /// will default to `DELTA`
    pub fn new<S: Into<String>>(
        schema: Schema,
        name: S,
        id: Option<S>,
        storage_path: S,
        table_format: Option<S>,
    ) -> Self {
        let format = table_format
            .map(Into::into)
            .unwrap_or(String::from("DELTA"));
        Self {
            schema,
            name: name.into(),
            storage_path: storage_path.into(),
            id: id.map(Into::into),
            format,
        }
    }

    /// Returns the name of the share associated with `self`
    ///
    /// # Example
    ///
    /// ```rust
    /// use delta_sharing_server::protocol::securable::{Share, Schema, Table};
    ///
    /// let share = Share::new("my-share", None);
    /// let schema = Schema::new(share, "my-schema", None);
    /// let table = Table::new(schema, "my-table", None, "my-storage-path", None);
    /// assert_eq!(table.share_name(), "my-share");
    /// ```
    pub fn share_name(&self) -> &str {
        self.schema.share_name()
    }

    /// Returns the id of the share associated with `self`
    ///
    /// # Example
    ///
    /// ```rust
    /// use delta_sharing_server::protocol::securable::{Share, Schema, Table};
    ///
    /// let share = Share::new("my-share", Some("my-share-id"));
    /// let schema = Schema::new(share, "my-schema", None);
    /// let table = Table::new(schema, "my-table", None, "my-storage-path", None);
    /// assert_eq!(table.share_id(), Some("my-share-id"));
    /// ```
    pub fn share_id(&self) -> Option<&str> {
        self.schema.share_id()
    }

    /// Returns the name of the schema associated with `self`
    ///
    /// # Example
    ///
    /// ```rust
    /// use delta_sharing_server::protocol::securable::{Share, Schema, Table};
    ///
    /// let share = Share::new("my-share", None);
    /// let schema = Schema::new(share, "my-schema", None);
    /// let table = Table::new(schema, "my-table", None, "my-storage-path", None);
    /// assert_eq!(table.schema_name(), "my-schema");
    /// ```
    pub fn schema_name(&self) -> &str {
        self.schema.name()
    }

    /// Returns the id of the schema associated with `self`
    ///
    /// # Example
    ///
    /// ```rust
    /// use delta_sharing_server::protocol::securable::{Share, Schema, Table};
    ///
    /// let share = Share::new("my-share", None);
    /// let schema = Schema::new(share, "my-schema", Some("my-schema-id"));
    /// let table = Table::new(schema, "my-table", None, "my-storage-path", None);
    /// assert_eq!(table.schema_id(), Some("my-schema-id"));
    /// ```
    pub fn schema_id(&self) -> Option<&str> {
        self.schema.id()
    }

    /// Returns the name of `self`
    ///
    /// # Example
    ///
    /// ```rust
    /// use delta_sharing_server::protocol::securable::{Share, Schema, Table};
    ///
    /// let share = Share::new("my-share", None);
    /// let schema = Schema::new(share, "my-schema", None);
    /// let table = Table::new(schema, "my-table", None, "my-storage-path", None);
    /// assert_eq!(table.name(), "my-table");
    /// ```
    pub fn name(&self) -> &str {
        self.name.as_ref()
    }

    /// Returns the id of `self`
    ///
    /// # Example
    ///
    /// ```rust
    /// use delta_sharing_server::protocol::securable::{Share, Schema, Table};
    ///
    /// let share = Share::new("my-share", None);
    /// let schema = Schema::new(share, "my-schema", None);
    /// let table = Table::new(schema, "my-table", Some("my-table-id"), "my-storage-path", None);
    /// assert_eq!(table.id(), Some("my-table-id"));
    /// ```
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    /// Returns the storage path of `self`
    ///
    /// # Example
    ///
    /// ```rust
    /// use delta_sharing_server::protocol::securable::{Share, Schema, Table};
    ///
    /// let share = Share::new("my-share", None);
    /// let schema = Schema::new(share, "my-schema", None);
    /// let table = Table::new(schema, "my-table", None, "my-storage-path", None);
    /// assert_eq!(table.storage_path(), "my-storage-path");
    /// ```
    pub fn storage_path(&self) -> &str {
        self.storage_path.as_ref()
    }

    /// Returns the format of `self`
    ///
    /// # Example
    ///    
    /// ```rust
    /// use delta_sharing_server::protocol::securable::{Share, Schema, Table};
    ///
    /// let share = Share::new("my-share", None);
    /// let schema = Schema::new(share, "my-schema", None);
    /// let table = Table::new(schema, "my-table", None, "my-storage-path", Some("parquet"));
    /// assert_eq!(table.format(), "parquet");
    ///
    /// let share = Share::new("my-share", None);
    /// let schema = Schema::new(share, "my-schema", None);
    /// let table = Table::new(schema, "my-table", None, "my-storage-path", None);
    /// assert_eq!(table.format(), "DELTA");
    /// ```
    pub fn format(&self) -> &str {
        self.format.as_ref()
    }
}

impl Display for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}.{}.{}",
            self.share_name(),
            self.schema_name(),
            self.name()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_share() {
        let share = Share::new("share", Some("id"));
        assert_eq!(format!("{}", share), "share");
    }

    #[test]
    fn display_schema() {
        let share = Share::new("share", Some("share_id"));
        let schema = Schema::new(share, "schema", Some("schema_id"));
        assert_eq!(format!("{}", schema), "share.schema");
    }

    #[test]
    fn display_table() {
        let share = Share::new("share", Some("share_id"));
        let schema = Schema::new(share, "schema", Some("schema_id"));
        let table = Table::new(
            schema,
            "table",
            Some("table_id"),
            "storage_path",
            Some("format"),
        );
        assert_eq!(format!("{}", table), "share.schema.table");
    }
}
