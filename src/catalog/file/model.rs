use serde::{Deserialize, Serialize};

use crate::catalog::{Schema, Share, Table};

#[derive(Debug, Serialize, Deserialize)]
pub struct ShareFile {
    shares: Vec<ShareConfig>,
}

impl ShareFile {
    pub fn new() -> Self {
        Self { shares: vec![] }
    }

    pub fn list_shares(&self, recipient: &str) -> Vec<Share> {
        self.shares
            .iter()
            .filter(|cfg| match &cfg.recipients {
                Some(r) => r.iter().any(|r| r == recipient),
                None => true,
            })
            .map(|cfg| cfg.to_share())
            .collect()
    }

    pub fn list_schemas(&self, recipient: &str, share_name: &str) -> Vec<Schema> {
        self.shares
            .iter()
            .filter(|share_cfg| match &share_cfg.recipients {
                Some(r) => r.iter().any(|r| r == recipient),
                None => true,
            })
            .filter(|share_cfg| share_cfg.name == share_name)
            .flat_map(|share_cfg| share_cfg.schemas())
            .map(|schema_cfg| schema_cfg.to_schema(share_name))
            .collect()
    }

    pub fn list_tables_in_share(&self, recipient: &str, share_name: &str) -> Vec<Table> {
        self.shares
            .iter()
            .filter(|share_cfg| match &share_cfg.recipients {
                Some(r) => r.iter().any(|r| r == recipient),
                None => true,
            })
            .filter(|share_cfg| share_cfg.name == share_name)
            .flat_map(|share_cfg| share_cfg.schemas())
            .flat_map(|schema_cfg| {
                std::iter::repeat(&schema_cfg.name).zip(schema_cfg.tables().iter())
            })
            .map(|(schema_name, table_cfg)| table_cfg.to_table(share_name, schema_name))
            .collect()
    }

    pub fn list_tables_in_schema(
        &self,
        recipient: &str,
        share_name: &str,
        schema_name: &str,
    ) -> Vec<Table> {
        self.shares
            .iter()
            .filter(|share_cfg| match &share_cfg.recipients {
                Some(r) => r.iter().any(|r| r == recipient),
                None => true,
            })
            .filter(|share_cfg| share_cfg.name == share_name)
            .flat_map(|share_cfg| share_cfg.schemas())
            .filter(|schema_cfg| schema_cfg.name == schema_name)
            .flat_map(|schema_cfg| schema_cfg.tables())
            .map(|table_cfg| table_cfg.to_table(share_name, schema_name))
            .collect()
    }
}

impl Default for ShareFile {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ShareConfig {
    name: String,
    schemas: Vec<SchemaConfig>,
    recipients: Option<Vec<String>>,
}

impl ShareConfig {
    fn to_share(&self) -> Share {
        Share::builder()
            .name(&self.name)
            .build()
            .expect("valid share")
    }

    fn schemas(&self) -> &[SchemaConfig] {
        &self.schemas
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct SchemaConfig {
    name: String,
    tables: Vec<TableConfig>,
}

impl SchemaConfig {
    fn tables(&self) -> &[TableConfig] {
        &self.tables
    }

    fn to_schema(&self, share_name: &str) -> Schema {
        Schema::builder()
            .name(&self.name)
            .share_name(share_name)
            .build()
            .expect("valid schema")
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct TableConfig {
    name: String,
    location: String,
    id: Option<String>,
}

impl TableConfig {
    fn to_table(&self, share_name: &str, schema_name: &str) -> Table {
        Table::builder()
            .name(&self.name)
            .storage_path(&self.location)
            .set_id(self.id.clone())
            .schema_name(schema_name)
            .share_name(share_name)
            .build()
            .expect("valid table")
    }
}
