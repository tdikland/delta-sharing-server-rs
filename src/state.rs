use std::{collections::HashMap, sync::Arc};

use crate::{
    error::ServerError,
    manager::{ListCursor, TableManager},
    reader::{delta::DeltaReader, TableReader, Version},
    response::{
        GetShareResponse, ListSchemasResponse, ListSharesResponse, ListTablesResponse,
        TableVersionResponse,
    },
    signer::UrlSigner,
};

#[derive(Clone)]
pub struct SharingServerState {
    shared_table_manager: Arc<dyn TableManager>,
    table_readers: HashMap<String, Arc<dyn TableReader>>,
    url_signers: HashMap<String, Arc<dyn UrlSigner>>,
}

impl SharingServerState {
    pub fn new(manager: Arc<dyn TableManager>) -> Self {
        let mut state = Self {
            shared_table_manager: manager,
            table_readers: HashMap::new(),
            url_signers: HashMap::new(),
        };

        // TODO: register default?
        let delta_reader = DeltaReader {};
        state
            .table_readers
            .insert("DELTA".to_owned(), Arc::new(delta_reader));

        state
    }

    pub fn add_table_reader(&mut self, format: impl Into<String>, reader: Arc<dyn TableReader>) {
        self.table_readers.insert(format.into(), reader);
    }

    pub fn add_url_signer(&mut self, storage: impl Into<String>, signer: Arc<dyn UrlSigner>) {
        self.url_signers.insert(storage.into(), signer);
    }

    pub fn table_manager(&self) -> Arc<dyn TableManager> {
        self.shared_table_manager.clone()
    }

    pub fn table_reader(&self, format: &str) -> Option<Arc<dyn TableReader>> {
        self.table_readers.get(format).cloned()
    }

    pub fn url_signer(&self, storage: &str) -> Option<Arc<dyn UrlSigner>> {
        self.url_signers.get(storage).cloned()
    }

    pub async fn list_shares(&self, cursor: ListCursor) -> Result<ListSharesResponse, ServerError> {
        let shares = self.shared_table_manager.list_shares(&cursor).await?;
        Ok(shares.into())
    }

    pub async fn get_share(&self, share_name: &str) -> Result<GetShareResponse, ServerError> {
        let share = self.shared_table_manager.get_share(share_name).await?;
        Ok(share.into())
    }

    pub async fn list_schemas(
        &self,
        share_name: &str,
        cursor: ListCursor,
    ) -> Result<ListSchemasResponse, ServerError> {
        let schemas = self
            .shared_table_manager
            .list_schemas(share_name, &cursor)
            .await?;
        Ok(schemas.into())
    }

    pub async fn list_tables_in_share(
        &self,
        share_name: &str,
        cursor: ListCursor,
    ) -> Result<ListTablesResponse, ServerError> {
        let tables = self
            .shared_table_manager
            .list_tables_in_share(share_name, &cursor)
            .await?;
        Ok(tables.into())
    }

    pub async fn list_tables_in_schema(
        &self,
        share_name: &str,
        schema_name: &str,
        cursor: ListCursor,
    ) -> Result<ListTablesResponse, ServerError> {
        let tables = self
            .shared_table_manager
            .list_tables_in_schema(share_name, schema_name, &cursor)
            .await?;
        Ok(tables.into())
    }

    pub async fn get_table_version(
        &self,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
        version: Version,
    ) -> Result<TableVersionResponse, ServerError> {
        let table = self
            .shared_table_manager
            .get_table(share_name, schema_name, table_name)
            .await?;
        let table_version = self
            .table_reader(table.format())
            .ok_or_else(|| ServerError::UnsupportedTableFormat {
                format: table.format().to_owned(),
            })?
            .get_table_version(table.storage_path(), version)
            .await?;

        Ok(table_version.into())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        manager::{List, MockTableManager, TableManagerError},
        protocol::securables::Share,
    };
    use insta::assert_json_snapshot;
    use mockall::predicate::eq;

    #[tokio::test]
    async fn test_list_shares() {
        let mut mock_table_manager = MockTableManager::new();
        mock_table_manager
            .expect_list_shares()
            .once()
            .returning(|_| {
                let mut shares = List::new(vec![], Some("continuation_token".to_owned()));
                shares.push(Share {
                    name: "vaccine_share".to_owned(),
                    id: Some("edacc4a7-6600-4fbb-85f3-a62a5ce6761f".to_owned()),
                });
                shares.push(Share {
                    name: "sales_share".to_owned(),
                    id: Some("3e979c79-6399-4dac-bcf8-54e268f48515".to_owned()),
                });
                Ok(shares)
            });

        let state = SharingServerState::new(Arc::new(mock_table_manager));
        let response = state.list_shares(ListCursor::default()).await.unwrap();
        assert_json_snapshot!(response);
    }

    #[tokio::test]
    async fn test_list_shares_with_pagination() {
        let mut mock_table_manager = MockTableManager::new();
        // Get 1 item with continuation token
        mock_table_manager
            .expect_list_shares()
            .with(eq(ListCursor::new(None, None)))
            .returning(|_| {
                let mut shares = List::new(vec![], Some("continuation_token".to_owned()));
                shares.push(Share {
                    name: "vaccine_share".to_owned(),
                    id: Some("edacc4a7-6600-4fbb-85f3-a62a5ce6761f".to_owned()),
                });
                Ok(shares)
            });

        // Get last item with no continuation token
        mock_table_manager
            .expect_list_shares()
            .with(eq(ListCursor::new(
                None,
                Some("continuation_token".to_owned()),
            )))
            .returning(|_| {
                let mut shares = List::new(vec![], None);
                shares.push(Share {
                    name: "sales_share".to_owned(),
                    id: Some("3e979c79-6399-4dac-bcf8-54e268f48515".to_owned()),
                });
                Ok(shares)
            });

        let state = SharingServerState::new(Arc::new(mock_table_manager));
        let response1 = state.list_shares(ListCursor::default()).await.unwrap();
        assert_json_snapshot!(response1);

        let response2 = state
            .list_shares(ListCursor::new(None, Some("continuation_token".to_owned())))
            .await
            .unwrap();
        assert_json_snapshot!(response2);
    }

    #[tokio::test]
    async fn test_get_share() {
        let mut mock_table_manager = MockTableManager::new();
        mock_table_manager
            .expect_get_share()
            .with(eq("vaccine_share"))
            .once()
            .returning(|_| {
                Ok(Share::new(
                    "vaccine_share".to_owned(),
                    Some("edacc4a7-6600-4fbb-85f3-a62a5ce6761f".to_owned()),
                ))
            });

        let state = SharingServerState::new(Arc::new(mock_table_manager));
        let response = state.get_share("vaccine_share").await.unwrap();
        assert_json_snapshot!(response);
    }

    #[tokio::test]
    async fn test_get_share_not_found() {
        let mut mock_table_manager = MockTableManager::new();
        mock_table_manager
            .expect_get_share()
            .with(eq("vaccine_share"))
            .once()
            .returning(|_| {
                Err(TableManagerError::ShareNotFound {
                    share_name: "vaccine_share".to_owned(),
                })
            });

        let state = SharingServerState::new(Arc::new(mock_table_manager));
        let response = state.get_share("vaccine_share").await;
        assert!(response.is_err());
        assert_eq!(
            response.unwrap_err(),
            ServerError::ShareNotFound {
                name: "vaccine_share".to_owned()
            }
        );
    }

    #[tokio::test]
    async fn test_list_schemas() {
        let mut mock_table_manager = MockTableManager::new();
        mock_table_manager
            .expect_list_schemas()
            .once()
            .returning(|| {
                let mut schemas = List::new(vec![], None);
                schemas.push("vaccine_schema".to_owned());
                schemas.push("sales_schema".to_owned());
                Ok(schemas)
            });

        let state = SharingServerState::new(Arc::new(mock_table_manager));
        let response = state.list_schemas().await.unwrap();
        assert_json_snapshot!(response);
    }
}
