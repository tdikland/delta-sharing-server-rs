//! Catalog implementation based on a configuration file.

use self::model::ShareFile;

use super::{Catalog, CatalogError, Page, Pagination, Schema, Share, Table};
use crate::auth::RecipientId;

mod config;
mod model;

pub use config::FileCatalogConfig;

/// Catalog based on a configuration file.
#[derive(Debug)]
pub struct FileCatalog {
    config: FileCatalogConfig,
    shares: ShareFile,
}

impl FileCatalog {
    /// Creates a new instance of the FileShareManager.
    pub fn new(config: FileCatalogConfig) -> Self {
        let mut this = Self {
            config,
            shares: Default::default(),
        };
        this.load().expect("configuration file could not be loaded");
        this
    }

    fn load(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let handle = std::fs::OpenOptions::new()
            .read(true)
            .open(self.config.path())?;
        let shares: ShareFile = serde_yaml::from_reader::<_, ShareFile>(handle)?;
        self.shares = shares;
        Ok(())
    }

    fn _flush(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }

    fn file(&self) -> &ShareFile {
        &self.shares
    }
}

#[async_trait::async_trait]
impl Catalog for FileCatalog {
    async fn list_shares(
        &self,
        recipient_id: &RecipientId,
        pagination: &Pagination,
    ) -> Result<Page<Share>, CatalogError> {
        let (offset, max_results) = parse_pagination(pagination)?;
        let shares = self.file().list_shares(recipient_id);

        let page = if offset + max_results >= shares.len() {
            Page::new(shares[offset..].to_vec(), None)
        } else {
            Page::new(
                shares[offset..offset + max_results].to_vec(),
                Some((offset + max_results).to_string()),
            )
        };

        Ok(page)
    }

    async fn get_share(
        &self,
        recipient_id: &RecipientId,
        share_name: &str,
    ) -> Result<Share, CatalogError> {
        self.file()
            .list_shares(recipient_id)
            .into_iter()
            .find(|share| share.name() == share_name)
            .ok_or(CatalogError::not_found("Share not found"))
    }

    async fn list_schemas(
        &self,
        recipient_id: &RecipientId,
        share_name: &str,
        pagination: &Pagination,
    ) -> Result<Page<Schema>, CatalogError> {
        let (offset, max_results) = parse_pagination(pagination)?;
        let schemas = self.file().list_schemas(recipient_id, share_name);

        let page = if offset + max_results >= schemas.len() {
            Page::new(schemas[offset..].to_vec(), None)
        } else {
            Page::new(
                schemas[offset..offset + max_results].to_vec(),
                Some((offset + max_results).to_string()),
            )
        };

        Ok(page)
    }

    async fn list_tables_in_share(
        &self,
        recipient_id: &RecipientId,
        share_name: &str,
        pagination: &Pagination,
    ) -> Result<Page<Table>, CatalogError> {
        let (offset, max_results) = parse_pagination(pagination)?;
        let tables = self.file().list_tables_in_share(recipient_id, share_name);

        let page = if offset + max_results >= tables.len() {
            Page::new(tables[offset..].to_vec(), None)
        } else {
            Page::new(
                tables[offset..offset + max_results].to_vec(),
                Some((offset + max_results).to_string()),
            )
        };

        Ok(page)
    }

    async fn list_tables_in_schema(
        &self,
        recipient_id: &RecipientId,
        share_name: &str,
        schema_name: &str,
        pagination: &Pagination,
    ) -> Result<Page<Table>, CatalogError> {
        let (offset, max_results) = parse_pagination(pagination)?;
        let tables = self
            .file()
            .list_tables_in_schema(recipient_id, share_name, schema_name);

        let page = if offset + max_results >= tables.len() {
            Page::new(tables[offset..].to_vec(), None)
        } else {
            Page::new(
                tables[offset..offset + max_results].to_vec(),
                Some((offset + max_results).to_string()),
            )
        };

        Ok(page)
    }

    async fn get_table(
        &self,
        recipient_id: &RecipientId,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Table, CatalogError> {
        self.file()
            .list_tables_in_schema(recipient_id, share_name, schema_name)
            .into_iter()
            .find(|table| table.name() == table_name)
            .ok_or(CatalogError::not_found("table not found"))
    }
}

fn parse_pagination(p: &Pagination) -> Result<(usize, usize), CatalogError> {
    let offset = p
        .page_token()
        .map(|token| {
            token.parse::<usize>().map_err({
                |e| {
                    tracing::error!(pagination = ?token, error = ?e, "invalid page token");
                    CatalogError::malformed_pagination("Invalid page token")
                }
            })
        })
        .transpose()?
        .unwrap_or(0);
    let max_results = p.max_results().unwrap_or(500) as usize;

    Ok((offset, max_results))
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

    #[tokio::test]
    async fn list_shares() {
        let tempfile = setup_share_config_file();
        let config = FileCatalogConfig::new(tempfile.path());

        let catalog = FileCatalog::new(config);

        let page = catalog
            .list_shares(&RecipientId::Anonymous, &Pagination::new(Some(2), None))
            .await
            .unwrap();
        assert_eq!(page.items.len(), 2);
        assert_eq!(page.next_page_token(), Some("2"));

        let page = catalog
            .list_shares(
                &RecipientId::Anonymous,
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
        let config = FileCatalogConfig::new(tempfile.path());

        let catalog = FileCatalog::new(config);
        let share = catalog
            .get_share(&RecipientId::Anonymous, "share1")
            .await
            .unwrap();
        assert_eq!(share.name(), "share1");
    }

    #[tokio::test]
    async fn list_schemas() {
        let tempfile = setup_share_config_file();
        let config = FileCatalogConfig::new(tempfile.path());

        let catalog = FileCatalog::new(config);
        let schemas = catalog
            .list_schemas(&RecipientId::Anonymous, "share1", &Pagination::default())
            .await
            .unwrap();
        assert_eq!(schemas.items.len(), 1);
        assert_eq!(schemas.items[0].name(), "schema1");
        assert_eq!(schemas.items[0].share_name(), "share1");
        assert_eq!(schemas.items[0].id(), None);
    }

    #[tokio::test]
    async fn list_tables_in_share() {
        let tempfile = setup_share_config_file();
        let config = FileCatalogConfig::new(tempfile.path());

        let catalog = FileCatalog::new(config);
        let tables = catalog
            .list_tables_in_share(&RecipientId::Anonymous, "share1", &Pagination::default())
            .await
            .unwrap();
        assert_eq!(tables.items.len(), 2);
        assert_eq!(tables.items[0].name(), "table1");
        assert_eq!(tables.items[0].schema_name(), "schema1");
        assert_eq!(tables.items[0].share_name(), "share1");
        assert_eq!(
            tables.items[0].storage_path(),
            "s3a://<bucket-name>/<the-table-path>"
        );
    }

    #[tokio::test]
    async fn list_tables_in_schema() {
        let tempfile = setup_share_config_file();
        let config = FileCatalogConfig::new(tempfile.path());

        let catalog = FileCatalog::new(config);
        let tables = catalog
            .list_tables_in_schema(
                &RecipientId::Anonymous,
                "share1",
                "schema1",
                &Pagination::default(),
            )
            .await
            .unwrap();
        assert_eq!(tables.items.len(), 2);
        assert_eq!(tables.items[0].name(), "table1");
        assert_eq!(tables.items[0].schema_name(), "schema1");
        assert_eq!(tables.items[0].share_name(), "share1");
        assert_eq!(
            tables.items[0].storage_path(),
            "s3a://<bucket-name>/<the-table-path>"
        );
    }

    #[tokio::test]
    async fn get_table() {
        let tempfile = setup_share_config_file();
        let config = FileCatalogConfig::new(tempfile.path());

        let catalog = FileCatalog::new(config);
        let tables = catalog
            .get_table(&RecipientId::Anonymous, "share1", "schema1", "table1")
            .await
            .unwrap();
        assert_eq!(tables.name(), "table1");
        assert_eq!(tables.schema_name(), "schema1");
        assert_eq!(tables.share_name(), "share1");
        assert_eq!(
            tables.storage_path(),
            "s3a://<bucket-name>/<the-table-path>"
        );
        assert_eq!(tables.id(), None);
        assert_eq!(tables.share_id(), None);
    }
}
