//! TableManager implementation using a local file.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::protocol::{
    securable::{Schema, SchemaBuilder, Share, ShareBuilder, Table, TableBuilder},
    share::{List, ListCursor},
};

use crate::manager::ShareIoError;

use super::ShareReader;

/// The file format where the share configuration is stored.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum FileFormat {
    /// Json file format
    Json,
    /// Yaml file format
    Yaml,
    /// Toml file format
    Toml,
}

/// ShareReader using a configuration file as the backing store.
#[derive(Debug)]
pub struct FileShareManager {
    path: PathBuf,
    format: FileFormat,
    share_file: ShareConfig,
}

impl FileShareManager {
    /// Creates a new instance of the FileShareManager.
    pub fn new(path: PathBuf) -> Self {
        let shares_file = Self::read_from_file(&path).unwrap();

        Self {
            path,
            format: FileFormat::Yaml,
            share_file: shares_file,
        }
    }

    /// Returns the path to the file that contains the configuration of the shared securables.
    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    /// Returns the format of the file that contains the configuration of the shared securables.
    pub fn format(&self) -> FileFormat {
        self.format
    }

    fn read_from_file(path: &PathBuf) -> Result<ShareConfig, ShareIoError> {
        let handle = std::fs::OpenOptions::new().read(true).open(path).unwrap();
        let shares_file: ShareConfig = serde_yaml::from_reader(handle).unwrap();
        Ok(shares_file)
    }

    fn _write_to_file(&self) -> Result<(), ShareIoError> {
        let handle = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&self.path)
            .expect("Couldn't open file");
        serde_yaml::to_writer(handle, &self.share_file).unwrap();
        Ok(())
    }
}

#[async_trait::async_trait]
impl ShareReader for FileShareManager {
    async fn list_shares(&self, _cursor: &ListCursor) -> Result<List<Share>, ShareIoError> {
        let shares = self.share_file.shares();
        Ok(List::new(shares, None))
    }

    async fn get_share(&self, share_name: &str) -> Result<Share, ShareIoError> {
        self.share_file
            .shares()
            .into_iter()
            .find(|share| share.name() == share_name)
            .ok_or(ShareIoError::ShareNotFound {
                share_name: share_name.to_string(),
            })
    }

    async fn list_schemas(
        &self,
        share_name: &str,
        _cursor: &ListCursor,
    ) -> Result<List<Schema>, ShareIoError> {
        let schemas = self.share_file.schemas(share_name);
        Ok(List::new(schemas, None))
    }

    async fn list_tables_in_share(
        &self,
        share_name: &str,
        _cursor: &ListCursor,
    ) -> Result<List<Table>, ShareIoError> {
        let tables = self.share_file.tables(share_name, None);
        Ok(List::new(tables, None))
    }

    async fn list_tables_in_schema(
        &self,
        share_name: &str,
        schema_name: &str,
        _cursor: &ListCursor,
    ) -> Result<List<Table>, ShareIoError> {
        let tables = self.share_file.tables(share_name, Some(schema_name));
        Ok(List::new(tables, None))
    }

    async fn get_table(
        &self,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Table, ShareIoError> {
        let tables = self.share_file.tables(share_name, Some(schema_name));
        tables
            .into_iter()
            .find(|table| table.name() == table_name)
            .ok_or(ShareIoError::TableNotFound {
                share_name: share_name.to_string(),
                schema_name: schema_name.to_string(),
                table_name: table_name.to_string(),
            })
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ShareConfig {
    shares: Vec<ShareInFile>,
}

impl ShareConfig {
    fn shares(&self) -> Vec<Share> {
        self.shares
            .iter()
            .map(|share| ShareBuilder::new(&share.name).build())
            .collect()
    }

    fn schemas(&self, share_name: &str) -> Vec<Schema> {
        if let Some(share) = self.shares.iter().find(|share| share.name == share_name) {
            share
                .schemas
                .iter()
                .map(|schema| {
                    let share = ShareBuilder::new(&share.name).build();
                    SchemaBuilder::new(share, &schema.name).build()
                })
                .collect()
        } else {
            vec![]
        }
    }

    fn tables(&self, share_name: &str, schema_name: Option<&str>) -> Vec<Table> {
        if let Some(share) = self.shares.iter().find(|share| share.name == share_name) {
            share
                .schemas
                .iter()
                .filter(|s| {
                    if let Some(n) = schema_name {
                        s.name == n
                    } else {
                        true
                    }
                })
                .flat_map(|schema| {
                    schema.tables.iter().map(|table| {
                        let share = ShareBuilder::new(&share.name).build();
                        let schema = SchemaBuilder::new(share, &schema.name).build();
                        TableBuilder::new(schema, &table.name, &table.location)
                            .id(&table.id)
                            .build()
                    })
                })
                .collect()
        } else {
            vec![]
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ShareInFile {
    name: String,
    schemas: Vec<SchemaInFile>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SchemaInFile {
    name: String,
    tables: Vec<TableInFile>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TableInFile {
    name: String,
    location: String,
    id: String,
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;

    use crate::protocol::securable::ShareBuilder;

    use super::*;

    fn setup_share_config_file() -> NamedTempFile {
        let mut temp_file = NamedTempFile::new().unwrap();
        let shares_config = r#"shares:
- name: "share1"
  schemas:
  - name: "schema1"
    tables:
    - name: "table1"
      location: "s3a://<bucket-name>/<the-table-path>"
      id: "00000000-0000-0000-0000-000000000000"
    - name: "table2"
      location: "wasbs://<container-name>@<account-name}.blob.core.windows.net/<the-table-path>"
      id: "00000000-0000-0000-0000-000000000001"
- name: "share2"
  schemas:
  - name: "schema2"
    tables:
    - name: "table3"
      location: "abfss://<container-name>@<account-name}.dfs.core.windows.net/<the-table-path>"
      cdfEnabled: true
      id: "00000000-0000-0000-0000-000000000002"
- name: "share3"
  schemas:
  - name: "schema3"
    tables:
    - name: "table4"
      location: "gs://<bucket-name>/<the-table-path>"
      id: "00000000-0000-0000-0000-000000000003"
- name: "share4"
  schemas:
  - name: "schema4"
    tables:
    - name: "table5"
      location: "s3a://<bucket-name>/<the-table-path>"
      id: "00000000-0000-0000-0000-000000000004""#;
        temp_file.write_all(shares_config.as_bytes()).unwrap();
        temp_file
    }

    #[tokio::test]
    async fn list_shares() {
        let tempfile = setup_share_config_file();
        let manager = FileShareManager::new(tempfile.path().to_path_buf());
        assert_eq!(
            manager.list_shares(&ListCursor::default()).await.unwrap(),
            List::new(
                vec![
                    ShareBuilder::new("share1").build(),
                    ShareBuilder::new("share2").build(),
                    ShareBuilder::new("share3").build(),
                    ShareBuilder::new("share4").build(),
                ],
                None
            )
        );
    }

    #[tokio::test]
    async fn get_share() {
        let tempfile = setup_share_config_file();
        let manager = FileShareManager::new(tempfile.path().to_path_buf());
        assert_eq!(
            manager.get_share("share1").await.unwrap(),
            ShareBuilder::new("share1").build()
        );
    }

    #[tokio::test]
    async fn list_schemas() {
        let tempfile = setup_share_config_file();
        let manager = FileShareManager::new(tempfile.path().to_path_buf());
        assert_eq!(
            manager
                .list_schemas("share1", &ListCursor::default())
                .await
                .unwrap(),
            List::new(
                vec![SchemaBuilder::new(ShareBuilder::new("share1").build(), "schema1").build()],
                None
            )
        );
    }

    #[tokio::test]
    async fn list_tables_in_share() {
        let tempfile = setup_share_config_file();
        let manager = FileShareManager::new(tempfile.path().to_path_buf());
        assert_eq!(
            manager
                .list_tables_in_share("share1"  , &ListCursor::default())
                .await
                .unwrap(),
            List::new(
                vec![
                    TableBuilder::new(
                        SchemaBuilder::new(ShareBuilder::new("share1").build(), "schema1").build(),
                        "table1",
                        "s3a://<bucket-name>/<the-table-path>"
                    )
                    .id("00000000-0000-0000-0000-000000000000")
                    .build(),
                    TableBuilder::new(
                        SchemaBuilder::new(ShareBuilder::new("share1").build(), "schema1").build(),
                        "table2",
                        "wasbs://<container-name>@<account-name}.blob.core.windows.net/<the-table-path>"
                    )
                    .id("00000000-0000-0000-0000-000000000001")
                    .build(),
                ],
                None
            )
        );
    }

    #[tokio::test]
    async fn list_tables_in_schema() {
        let tempfile = setup_share_config_file();
        let manager = FileShareManager::new(tempfile.path().to_path_buf());
        assert_eq!(
            manager
                .list_tables_in_schema("share1", "schema1", &ListCursor::default())
                .await
                .unwrap(),
            List::new(
                vec![
                    TableBuilder::new(
                        SchemaBuilder::new(ShareBuilder::new("share1").build(), "schema1").build(),
                        "table1",
                        "s3a://<bucket-name>/<the-table-path>"
                    )
                    .id("00000000-0000-0000-0000-000000000000")
                    .build(),
                    TableBuilder::new(
                        SchemaBuilder::new(ShareBuilder::new("share1").build(), "schema1").build(),
                        "table2",
                        "wasbs://<container-name>@<account-name}.blob.core.windows.net/<the-table-path>"
                    )
                    .id("00000000-0000-0000-0000-000000000001")
                    .build(),
                ],
                None
            )
        );
    }

    #[tokio::test]
    async fn get_table() {
        let tempfile = setup_share_config_file();
        let manager = FileShareManager::new(tempfile.path().to_path_buf());
        assert_eq!(
            manager
                .get_table("share1", "schema1", "table1")
                .await
                .unwrap(),
            TableBuilder::new(
                SchemaBuilder::new(ShareBuilder::new("share1").build(), "schema1").build(),
                "table1",
                "s3a://<bucket-name>/<the-table-path>"
            )
            .id("00000000-0000-0000-0000-000000000000")
            .build()
        );
    }
}
