use std::path::PathBuf;

use crate::protocol::{
    securable::{Schema, Share, Table},
    share::{List, ListCursor},
};

use super::ShareIoError;

#[derive(Debug)]
pub enum FileFormat {
    Json,
    Yaml,
    Toml,
}

#[derive(Debug)]
pub struct FileShareManager {
    path: PathBuf,
    format: FileFormat,
}

impl FileShareManager {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            format: FileFormat::Json,
        }
    }

    pub fn create_share(&mut self, share_name: &Share) -> Result<(), ShareIoError> {
        Ok(())
    }

    pub fn create_schema(&mut self, schema: &Schema) -> Result<(), ShareIoError> {
        Ok(())
    }

    pub fn create_table(&mut self, table: &Table) -> Result<(), ShareIoError> {
        Ok(())
    }

    pub fn get_share(&self, share_name: &str) -> Result<Share, ShareIoError> {
        todo!()
    }

    pub fn get_schema(&self, schema_name: &str) -> Result<Schema, ShareIoError> {
        todo!()
    }

    pub fn get_table(&self, table_name: &str) -> Result<Table, ShareIoError> {
        todo!()
    }

    pub fn list_shares(&self, cursor: &ListCursor) -> Result<List<Share>, ShareIoError> {
        todo!()
    }

    pub fn list_schemas(
        &self,
        share_name: &str,
        cursor: &ListCursor,
    ) -> Result<List<Schema>, ShareIoError> {
        todo!()
    }

    pub fn list_tables(
        &self,
        share_name: &str,
        schema_name: Option<&str>,
        cursor: &ListCursor,
    ) -> Result<List<Table>, ShareIoError> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::protocol::securable::ShareBuilder;

    use super::*;

    #[test]
    fn test_create_share() {
        let mut manager = FileShareManager::new(PathBuf::from("/tmp"));
        let share = ShareBuilder::new("test_share").build();
        manager.create_share(&share).unwrap();
    }
}
