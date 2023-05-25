use std::fmt::Display;

use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd)]
pub struct Share {
    name: String,
    id: Option<String>,
}

impl Share {
    pub fn new(name: String, id: Option<String>) -> Self {
        Self { name, id }
    }

    pub fn name(&self) -> &str {
        self.name.as_ref()
    }

    pub fn id(&self) -> Option<&String> {
        self.id.as_ref()
    }
}

impl Display for Share {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Hash)]
pub struct Schema {
    share: Share,
    name: String,
    id: Option<String>
}

impl Schema {
    pub fn new(share: Share, name: String, id: Option<String>) -> Self {
        Self { share, name, id: None }
    }

    pub fn share_name(&self) -> &str {
        self.share.name()
    }

    pub fn share_id(&self) -> Option<&String> {
        self.share.id()
    }

    pub fn name(&self) -> &str {
        self.name.as_ref()
    }

    pub fn id(&self) -> Option<&String> {
        self.id.as_ref()
    }
}

impl Display for Schema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.share_name(), self.name())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Hash)]
pub struct Table {
    schema: Schema,
    name: String,
    storage_path: String,
    table_id: Option<String>,
    format: String,
}

impl Table {
    pub fn new(
        schema: Schema,
        name: String,
        storage_path: String,
        table_id: Option<String>,
        table_format: Option<String>,
    ) -> Self {
        let format = table_format.unwrap_or(String::from("DELTA"));
        Self {
            schema,
            name,
            storage_path,
            table_id,
            format,
        }
    }

    pub fn share_name(&self) -> &str {
        self.schema.share_name()
    }

    pub fn share_id(&self) -> Option<&String> {
        self.schema.share_id()
    }

    pub fn schema_name(&self) -> &str {
        self.schema.name()
    }

    pub fn name(&self) -> &str {
        self.name.as_ref()
    }

    pub fn storage_path(&self) -> &str {
        self.storage_path.as_ref()
    }

    pub fn table_id(&self) -> Option<&String> {
        self.table_id.as_ref()
    }

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
