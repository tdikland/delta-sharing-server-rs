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

impl Share {
    /// Retrieve the name from `self`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use delta_sharing_server::protocol::securable::ShareBuilder;
    ///
    /// let share = ShareBuilder::new("my-share").build();
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
    /// use delta_sharing_server::protocol::securable::ShareBuilder;
    ///
    /// let share = ShareBuilder::new("my-share").id("my-share-id").build();
    /// assert_eq!(share.id(), Some("my-share-id"));
    /// ```
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }
}

/// Builder for [`Share`].
pub struct ShareBuilder {
    name: String,
    id: Option<String>,
}

impl ShareBuilder {
    /// Create a new `ShareBuilder`.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            id: None,
        }
    }

    /// Set the id of the share.
    pub fn id<S: Into<String>>(mut self, id: S) -> Self {
        self.id = Some(id.into());
        self
    }

    /// Set the id of the share.
    pub fn set_id<S: Into<String>>(mut self, id: Option<S>) -> Self {
        self.id = id.map(Into::into);
        self
    }

    /// Build the [`Share`].
    pub fn build(self) -> Share {
        Share {
            name: self.name,
            id: self.id,
        }
    }
}

impl Display for Share {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
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

impl Schema {
    /// Returns the name of the share associated with `self`
    ///
    /// # Example
    ///
    /// ```rust
    /// use delta_sharing_server::protocol::securable::{ShareBuilder, SchemaBuilder};
    ///
    /// let share = ShareBuilder::new("my-share").build();
    /// let schema = SchemaBuilder::new(share, "my-schema").build();
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
    /// use delta_sharing_server::protocol::securable::{ShareBuilder, SchemaBuilder};
    ///
    /// let share = ShareBuilder::new("my-share").id("my-share-id").build();
    /// let schema = SchemaBuilder::new(share, "my-schema").build();
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
    /// use delta_sharing_server::protocol::securable::{ShareBuilder, SchemaBuilder};
    ///
    /// let share = ShareBuilder::new("my-share").build();
    /// let schema = SchemaBuilder::new(share, "my-schema").build();
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
    /// use delta_sharing_server::protocol::securable::{ShareBuilder, SchemaBuilder};
    ///
    /// let share = ShareBuilder::new("my-share").build();
    /// let schema = SchemaBuilder::new(share, "my-schema").id("my-schema-id").build();
    /// assert_eq!(schema.id(), Some("my-schema-id"));
    /// ```
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }
}

/// Builder for [`Schema`].
pub struct SchemaBuilder {
    share: Share,
    name: String,
    id: Option<String>,
}

impl SchemaBuilder {
    /// Create a new `SchemaBuilder`.
    pub fn new(share: Share, schema_name: impl Into<String>) -> Self {
        Self {
            share,
            name: schema_name.into(),
            id: None,
        }
    }

    /// Set the id of the schema.
    pub fn id<S: Into<String>>(mut self, id: S) -> Self {
        self.id = Some(id.into());
        self
    }

    /// Set the id of the schema.
    pub fn set_id<S: Into<String>>(mut self, id: Option<S>) -> Self {
        self.id = id.map(Into::into);
        self
    }

    /// Build the [`Schema`].
    pub fn build(self) -> Schema {
        Schema {
            share: self.share,
            name: self.name,
            id: self.id,
        }
    }
}

impl Display for Schema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.share_name(), self.name())
    }
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

impl Table {
    /// Returns the name of the share associated with `self`
    ///
    /// # Example
    ///
    /// ```rust
    /// use delta_sharing_server::protocol::securable::{ShareBuilder, SchemaBuilder, TableBuilder};
    ///
    /// let share = ShareBuilder::new("my-share").build();
    /// let schema = SchemaBuilder::new(share, "my-schema").build();
    /// let table = TableBuilder::new(schema, "my-table", "my-storage-path").build();
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
    /// use delta_sharing_server::protocol::securable::{ShareBuilder, SchemaBuilder, TableBuilder};
    ///
    /// let share = ShareBuilder::new("my-share").id("my-share-id").build();
    /// let schema = SchemaBuilder::new(share, "my-schema").build();
    /// let table = TableBuilder::new(schema, "my-table", "my-storage-path").build();
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
    /// use delta_sharing_server::protocol::securable::{ShareBuilder, SchemaBuilder, TableBuilder};
    ///
    /// let share = ShareBuilder::new("my-share").build();
    /// let schema = SchemaBuilder::new(share, "my-schema").build();
    /// let table = TableBuilder::new(schema, "my-table", "my-storage-path").build();
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
    /// use delta_sharing_server::protocol::securable::{ShareBuilder, SchemaBuilder, TableBuilder};
    ///
    /// let share = ShareBuilder::new("my-share").build();
    /// let schema = SchemaBuilder::new(share, "my-schema").id("my-schema-id").build();
    /// let table = TableBuilder::new(schema, "my-table", "my-storage-path").build();
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
    /// use delta_sharing_server::protocol::securable::{ShareBuilder, SchemaBuilder, TableBuilder};
    ///
    /// let share = ShareBuilder::new("my-share").build();
    /// let schema = SchemaBuilder::new(share, "my-schema").build();
    /// let table = TableBuilder::new(schema, "my-table", "my-storage-path").build();
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
    /// use delta_sharing_server::protocol::securable::{ShareBuilder, SchemaBuilder, TableBuilder};
    ///
    /// let share = ShareBuilder::new("my-share").build();
    /// let schema = SchemaBuilder::new(share, "my-schema").build();
    /// let table = TableBuilder::new(schema, "my-table", "my-storage-path").id("my-table-id").build();
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
    /// use delta_sharing_server::protocol::securable::{ShareBuilder, SchemaBuilder, TableBuilder};
    ///
    /// let share = ShareBuilder::new("my-share").build();
    /// let schema = SchemaBuilder::new(share, "my-schema").build();
    /// let table = TableBuilder::new(schema, "my-table", "my-storage-path").build();
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
    /// use delta_sharing_server::protocol::securable::{ShareBuilder, SchemaBuilder, TableBuilder};
    ///
    /// let share = ShareBuilder::new("my-share").build();
    /// let schema = SchemaBuilder::new(share, "my-schema").build();
    /// let table = TableBuilder::new(schema, "my-table", "my-storage-path").format("PARQUET").build();
    /// assert_eq!(table.format(), "PARQUET");
    ///
    /// let share = ShareBuilder::new("my-share").build();
    /// let schema = SchemaBuilder::new(share, "my-schema").build();
    /// let table = TableBuilder::new(schema, "my-table", "my-storage-path").build();
    /// assert_eq!(table.format(), "DELTA");
    /// ```
    pub fn format(&self) -> &str {
        self.format.as_ref()
    }
}

/// Builder for `Table`
pub struct TableBuilder {
    schema: Schema,
    name: String,
    id: Option<String>,
    storage_path: String,
    format: Option<String>,
}

impl TableBuilder {
    /// Creates a new `TableBuilder` with the given `schema`, `table_name` and `storage_path`
    pub fn new(
        schema: Schema,
        table_name: impl Into<String>,
        storage_path: impl Into<String>,
    ) -> Self {
        Self {
            schema,
            name: table_name.into(),
            id: None,
            storage_path: storage_path.into(),
            format: None,
        }
    }

    /// Sets the id of the table
    pub fn id<S: Into<String>>(mut self, id: S) -> Self {
        self.id = Some(id.into());
        self
    }

    /// Sets the id of the table
    pub fn set_id<S: Into<String>>(mut self, id: Option<S>) -> Self {
        self.id = id.map(Into::into);
        self
    }

    /// Sets the format of the table
    pub fn format<S: Into<String>>(mut self, format: S) -> Self {
        self.format = Some(format.into());
        self
    }

    /// Sets the format of the table
    pub fn set_format<S: Into<String>>(mut self, format: Option<S>) -> Self {
        self.format = format.map(Into::into);
        self
    }

    /// Builds a `Table` from the current `TableBuilder`
    pub fn build(self) -> Table {
        Table {
            schema: self.schema,
            name: self.name,
            id: self.id,
            storage_path: self.storage_path,
            format: self.format.unwrap_or_else(|| "DELTA".to_string()),
        }
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
        let share = ShareBuilder::new("share").id("share_id").build();
        assert_eq!(format!("{}", share), "share");
    }

    #[test]
    fn display_schema() {
        let share = ShareBuilder::new("share").build();
        let schema = SchemaBuilder::new(share, "schema").build();
        assert_eq!(format!("{}", schema), "share.schema");
    }

    #[test]
    fn display_table() {
        let share = ShareBuilder::new("share").build();
        let schema = SchemaBuilder::new(share, "schema").build();
        let table = TableBuilder::new(schema, "table", "storage_path").build();
        assert_eq!(format!("{}", table), "share.schema.table");
    }
}
