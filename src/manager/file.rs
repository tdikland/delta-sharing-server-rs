use std::path::PathBuf;

use serde::{Deserialize, Serialize};

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
    share_file: ShareFile,
}

impl FileShareManager {
    pub fn new(path: PathBuf) -> Self {
        let shares_file = Self::read_from_file(&path).unwrap();

        Self {
            path,
            format: FileFormat::Yaml,
            share_file: shares_file,
        }
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    fn read_from_file(path: &PathBuf) -> Result<ShareFile, ShareIoError> {
        let handle = std::fs::OpenOptions::new()
            .read(true)
            .open(path)
            .unwrap();
        let shares_file: ShareFile =
            serde_yaml::from_reader(handle).expect("Could not read values.");

        Ok(shares_file)
    }

    fn write_to_file(&self) -> Result<(), ShareIoError> {
        let handle = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&self.path)
            .expect("Couldn't open file");
        serde_yaml::to_writer(handle, &self.share_file).unwrap();
        Ok(())
    }

    pub fn create_share(&mut self, share: &Share) -> Result<(), ShareIoError> {
        let new_share = ShareInFile {
            name: share.name().to_string(),
            schemas: vec![],
        };

        self.share_file.push(new_share);
        self.write_to_file()
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

type ShareFile = Vec<ShareInFile>;

#[derive(Debug, Serialize, Deserialize)]
struct ShareInFile {
    name: String,
    schemas: Vec<Schema>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SchemaInFile {
    name: String,
    tables: Vec<Table>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TableInFile {
    name: String,
    location: String,
    id: String,
}

#[cfg(test)]
mod tests {
    use crate::protocol::securable::ShareBuilder;

    use super::*;

    #[test]
    fn initialize_file_share_manager() {
        let manager = FileShareManager::new(PathBuf::from("./shared.yml"));
        assert_eq!(manager.path(), &PathBuf::from("./shared.yml"));
    }

    #[test]
    fn test_create_share() {
        let mut manager = FileShareManager::new(PathBuf::from("./share_me.yml"));
        let share = ShareBuilder::new("test_share").build();
        println!("{:?}", manager);
        manager.create_share(&share).unwrap();
        assert!(false);
    }
}
