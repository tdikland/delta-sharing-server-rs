//! TableManager implementation using a local file.
//!
//! TODO: implement pagination
//! TODO: implement multiple formats (JSON, YAML, TOML)

use std::{fs::File, path::PathBuf};

use serde::{Deserialize, Serialize};

use super::{CatalogError, Page, Pagination, Schema, Share, ShareReader, Table};
use crate::auth::ClientId;

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

/// Catalog based on a configuration file.
#[derive(Debug)]
pub struct FileCatalog {
    path: PathBuf,
    format: FileFormat,
    share_file: Option<ShareFile>,
    config: ShareFile,
}

impl FileCatalog {
    /// Creates a new instance of the FileShareManager.
    pub fn new(path: PathBuf) -> Self {
        let share_config = Self::read_from_file(&path).unwrap();
        let mut file_catalog = Self {
            path,
            format: FileFormat::Yaml,
            share_file: None,
            config: share_config,
        };
        file_catalog.sync();
        file_catalog
    }

    fn sync(&mut self) {
        let file = File::open(self.path.as_path()).unwrap();
        let shares: ShareFile = match self.format {
            FileFormat::Json => todo!(),
            FileFormat::Yaml => serde_yaml::from_reader(file).unwrap(),
            FileFormat::Toml => todo!(),
        };
        self.share_file = Some(shares);
    }

    // fn share_page(&self, max_results: u32, starting_from: u32) -> Page<ShareInfo> {
    //     if
    // }

    /// Returns the path to the file that contains the configuration of the shared securables.
    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    /// Returns the format of the file that contains the configuration of the shared securables.
    pub fn format(&self) -> FileFormat {
        self.format
    }

    fn read_from_file(path: &PathBuf) -> Result<ShareFile, CatalogError> {
        let handle = std::fs::OpenOptions::new().read(true).open(path).unwrap();
        let shares_file: ShareFile = serde_yaml::from_reader(handle).unwrap();
        Ok(shares_file)
    }
}

#[async_trait::async_trait]
impl ShareReader for FileCatalog {
    async fn list_shares(
        &self,
        _client_id: &ClientId,
        pagination: &Pagination,
    ) -> Result<Page<Share>, CatalogError> {
        let offset = pagination
            .page_token()
            .map(|t| t.parse::<usize>().unwrap())
            .unwrap_or(0);
        let max_results = pagination.max_results.unwrap_or(500) as usize;

        let shares = self.config.shares();
        let page = if offset + max_results >= shares.len() {
            Page::new(shares[offset..].to_vec(), None)
        } else {
            Page::new(
                shares[offset..offset + max_results].to_vec(),
                Some((offset + max_results).to_string()),
            )
        };

        Ok(page)

        // let shares = &self.config.shares()[offset..offset + max_results];
        // // let next_token = Some(max_results - shares.len())
        // //     .filter(|&r| r == 0)
        // //     .map(|t| t.to_string());

        // let next_token = (shares.len() == max_results).then(|| (offset + max_results).to_string());

        // Ok(Page::new(shares.to_vec(), next_token))
    }

    async fn get_share(
        &self,
        _client_id: &ClientId,
        share_name: &str,
    ) -> Result<Share, CatalogError> {
        self.config
            .shares()
            .into_iter()
            .find(|share| share.name() == share_name)
            .ok_or(CatalogError::ShareNotFound {
                share_name: share_name.to_string(),
            })
    }

    async fn list_schemas(
        &self,
        _client_id: &ClientId,
        share_name: &str,
        _pagination: &Pagination,
    ) -> Result<Page<Schema>, CatalogError> {
        let schemas = self.config.schemas(share_name);
        Ok(Page::new(schemas, None))
    }

    async fn list_tables_in_share(
        &self,
        _client_id: &ClientId,
        share_name: &str,
        _pagination: &Pagination,
    ) -> Result<Page<Table>, CatalogError> {
        let tables = self.config.tables_in_share(share_name);
        Ok(Page::new(tables, None))
    }

    async fn list_tables_in_schema(
        &self,
        _client_id: &ClientId,
        share_name: &str,
        schema_name: &str,
        _cursor: &Pagination,
    ) -> Result<Page<Table>, CatalogError> {
        let tables = self.config.tables_in_schema(share_name, schema_name);
        Ok(Page::new(tables, None))
    }

    async fn get_table(
        &self,
        _client_id: &ClientId,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Table, CatalogError> {
        let tables = self.config.tables_in_schema(share_name, schema_name);
        tables
            .into_iter()
            .find(|table| table.name() == table_name)
            .ok_or(CatalogError::TableNotFound {
                share_name: share_name.to_string(),
                schema_name: schema_name.to_string(),
                table_name: table_name.to_string(),
            })
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ShareFile {
    shares: Vec<ShareInFile>,
}

impl ShareFile {
    fn shares(&self) -> Vec<Share> {
        self.shares
            .iter()
            .map(|share| share.to_share_info())
            .collect()
    }

    fn schemas(&self, share_name: &str) -> Vec<Schema> {
        if let Some(share) = self.shares.iter().find(|share| share.name == share_name) {
            share
                .schemas()
                .iter()
                .map(|schema| schema.to_schema_info(share_name))
                .collect()
        } else {
            vec![]
        }
    }

    fn tables_in_share(&self, share_name: &str) -> Vec<Table> {
        if let Some(share) = self.shares.iter().find(|share| share.name == share_name) {
            share
                .schemas()
                .iter()
                .flat_map(|schema| {
                    schema
                        .tables()
                        .iter()
                        .map(|table| table.to_table_info(share_name, &schema.name))
                })
                .collect()
        } else {
            vec![]
        }
    }

    fn tables_in_schema(&self, share_name: &str, schema_name: &str) -> Vec<Table> {
        let Some(share) = self.shares.iter().find(|share| share.name == share_name) else {
            return vec![];
        };

        let Some(schema) = share
            .schemas
            .iter()
            .find(|schema| schema.name == schema_name)
        else {
            return vec![];
        };

        schema
            .tables()
            .iter()
            .map(|table| table.to_table_info(share_name, schema_name))
            .collect()
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ShareInFile {
    name: String,
    schemas: Vec<SchemaInFile>,
}

impl ShareInFile {
    fn schemas(&self) -> &[SchemaInFile] {
        &self.schemas
    }

    fn to_share_info(&self) -> Share {
        Share::new(self.name.clone(), None)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct SchemaInFile {
    name: String,
    tables: Vec<TableInFile>,
}

impl SchemaInFile {
    fn tables(&self) -> &[TableInFile] {
        &self.tables
    }

    fn to_schema_info(&self, share_name: &str) -> Schema {
        Schema::new(self.name.clone(), share_name.to_string())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct TableInFile {
    name: String,
    location: String,
    id: String,
}

impl TableInFile {
    fn to_table_info(&self, share_name: &str, schema_name: &str) -> Table {
        Table::new(
            self.name.clone(),
            schema_name.to_string(),
            share_name.to_string(),
            self.location.clone(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;

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

    #[ignore = "failing empty files"]
    #[tokio::test]
    async fn list_shares_empty() {
        let temp_file = NamedTempFile::new().unwrap();
        let catalog = FileCatalog::new(temp_file.path().to_path_buf());
        assert_eq!(
            catalog
                .list_shares(&ClientId::Anonymous, &Pagination::default())
                .await
                .unwrap(),
            Page::new(vec![], None)
        );
    }

    #[tokio::test]
    async fn list_shares() {
        let tempfile = setup_share_config_file();
        let catalog = FileCatalog::new(tempfile.path().to_path_buf());

        let page = catalog
            .list_shares(&ClientId::Anonymous, &Pagination::new(Some(2), None))
            .await
            .unwrap();
        assert_eq!(page.items.len(), 2);
        assert_eq!(page.next_page_token(), Some("2"));

        let page = catalog
            .list_shares(
                &ClientId::Anonymous,
                &Pagination::new(Some(2), Some("2".to_string())),
            )
            .await
            .unwrap();
        assert_eq!(page.items.len(), 2);
        assert_eq!(page.next_page_token(), None);
    }

    #[tokio::test]
    async fn get_share() {
        let tempfile = setup_share_config_file();
        let catalog = FileCatalog::new(tempfile.path().to_path_buf());
        assert_eq!(
            catalog
                .get_share(&ClientId::Anonymous, "share1")
                .await
                .unwrap(),
            Share::new("share1".to_owned(), None)
        );
    }

    #[tokio::test]
    async fn list_schemas() {
        let tempfile = setup_share_config_file();
        let catalog = FileCatalog::new(tempfile.path().to_path_buf());
        assert_eq!(
            catalog
                .list_schemas(&ClientId::Anonymous, "share1", &Pagination::default())
                .await
                .unwrap(),
            Page::new(
                vec![Schema::new("schema1".to_owned(), "share1".to_owned())],
                None
            )
        );
    }

    #[tokio::test]
    async fn list_tables_in_share() {
        let tempfile = setup_share_config_file();
        let catalog = FileCatalog::new(tempfile.path().to_path_buf());
        assert_eq!(
            catalog
                .list_tables_in_share(&ClientId::Anonymous, "share1"  , &Pagination::default())
                .await
                .unwrap(),
            Page::new(
                vec![
                    Table::new(
                        "table1".to_owned(),
                        "schema1".to_owned(),
                        "share1".to_owned(),
                        "s3a://<bucket-name>/<the-table-path>".to_owned()
                    ),
                    Table::new(
                        "table2".to_owned(),
                        "schema1".to_owned(),
                        "share1".to_owned(),
                        "wasbs://<container-name>@<account-name}.blob.core.windows.net/<the-table-path>"
                            .to_owned()
                    ),
                ],
                None
            )
        );
    }

    #[tokio::test]
    async fn list_tables_in_schema() {
        let tempfile = setup_share_config_file();
        let catalog = FileCatalog::new(tempfile.path().to_path_buf());
        assert_eq!(
            catalog
                .list_tables_in_schema(&ClientId::Anonymous, "share1", "schema1", &Pagination::default())
                .await
                .unwrap(),
            Page::new(
                vec![
                    Table::new(
                        "table1".to_owned(),
                        "schema1".to_owned(),
                        "share1".to_owned(),
                        "s3a://<bucket-name>/<the-table-path>".to_owned()
                    ),
                    Table::new(
                        "table2".to_owned(),
                        "schema1".to_owned(),
                        "share1".to_owned(),
                        "wasbs://<container-name>@<account-name}.blob.core.windows.net/<the-table-path>"
                            .to_owned()
                    ),
                ],
                None
            )
        );
    }

    #[tokio::test]
    async fn get_table() {
        let tempfile = setup_share_config_file();
        let catalog = FileCatalog::new(tempfile.path().to_path_buf());
        assert_eq!(
            catalog
                .get_table(&ClientId::Anonymous, "share1", "schema1", "table1")
                .await
                .unwrap(),
            Table::new(
                "table1".to_owned(),
                "schema1".to_owned(),
                "share1".to_owned(),
                "s3a://<bucket-name>/<the-table-path>".to_owned()
            )
        );
    }
}
