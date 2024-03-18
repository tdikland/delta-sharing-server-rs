//! Delta Sharing server state.

use std::sync::Arc;

use tracing::{debug, info, info_span, instrument};

use crate::{
    auth::RecipientId,
    catalog::{Catalog, Page, Pagination, Schema, Share, Table},
    error::ServerError,
    extract::{Capabilities, ResponseFormat},
    reader::{TableReader, TableVersionNumber, Version},
    response::{ListSharesResponse, TableActionsResponse},
    signer::{registry::SignerRegistry, UrlSigner},
};

/// State of the sharing server.
#[derive(Clone)]
pub struct SharingServerState {
    catalog: Arc<dyn Catalog>,
    reader: Arc<dyn TableReader>,
    signers: SignerRegistry,
}

impl SharingServerState {
    /// Create a new sharing server state.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        reader: Arc<dyn TableReader>,
        signers: SignerRegistry,
    ) -> Self {
        Self {
            catalog,
            reader,
            signers,
        }
    }

    /// Get the catalog from the state.
    pub fn catalog(&self) -> Arc<dyn Catalog> {
        self.catalog.clone()
    }

    /// Get the reader from the state.
    pub fn reader(&self) -> Arc<dyn TableReader> {
        self.reader.clone()
    }

    /// Add a url signer to the state.
    pub fn add_url_signer(&mut self, storage: impl Into<String>, signer: Arc<dyn UrlSigner>) {
        self.signers.register(&storage.into(), signer);
    }

    /// Get the url signer for a specific object store.
    pub fn url_signer(&self, storage: &str) -> Option<Arc<dyn UrlSigner>> {
        self.signers.get(storage)
    }

    /// Get a list of shares in the share store.
    pub async fn list_shares(
        &self,
        client_id: &RecipientId,
        pagination: &Pagination,
    ) -> Result<ListSharesResponse, ServerError> {
        self.catalog
            .list_shares(client_id, pagination)
            .await
            .map(ListSharesResponse::from)
            .map_err(Into::into)
    }

    /// Get a share from the share store.
    pub async fn get_share(
        &self,
        client_id: &RecipientId,
        share_name: &str,
    ) -> Result<Share, ServerError> {
        self.catalog
            .get_share(client_id, share_name)
            .await
            .map_err(Into::into)
    }

    /// Get a list of schemas in a share.
    pub async fn list_schemas(
        &self,
        client_id: &RecipientId,
        share_name: &str,
        pagination: &Pagination,
    ) -> Result<Page<Schema>, ServerError> {
        Ok(self
            .catalog
            .list_schemas(client_id, share_name, pagination)
            .await?)
    }

    /// Get a list of tables in a share.
    pub async fn list_tables_in_share(
        &self,
        client_id: &RecipientId,
        share_name: &str,
        pagination: &Pagination,
    ) -> Result<Page<Table>, ServerError> {
        Ok(self
            .catalog
            .list_tables_in_share(client_id, share_name, pagination)
            .await?)
    }

    /// Get a list of tables in a schema.
    pub async fn list_tables_in_schema(
        &self,
        client_id: &RecipientId,
        share_name: &str,
        schema_name: &str,
        pagination: &Pagination,
    ) -> Result<Page<Table>, ServerError> {
        Ok(self
            .catalog
            .list_tables_in_schema(client_id, share_name, schema_name, pagination)
            .await?)
    }

    /// Get the version of a table.
    pub async fn get_table_version_number(
        &self,
        client_id: &RecipientId,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
        version: Version,
    ) -> Result<TableVersionNumber, ServerError> {
        let table = self
            .catalog
            .get_table(client_id, share_name, schema_name, table_name)
            .await?;

        dbg!(&table);

        let table_version = self
            .reader
            .get_table_version_number(table.storage_path(), version)
            .await?;

        Ok(table_version)
    }

    /// Get the metadata of a table.
    pub async fn get_table_metadata(
        &self,
        client_id: &RecipientId,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
        _capabilities: &Capabilities,
    ) -> Result<TableActionsResponse, ServerError> {
        info!("fetching table from catalog");
        let table = self
            .catalog
            .get_table(client_id, share_name, schema_name, table_name)
            .await?;

        info!("reading delta log");
        let metadata = self.reader.get_table_meta(table.storage_path()).await?;

        Ok(TableActionsResponse::new_parquet(metadata))
    }

    /// Get the data files of a table version.
    pub async fn get_table_data(
        &self,
        client_id: &RecipientId,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
        _version: Version,
        capabilities: &Capabilities,
    ) -> Result<TableActionsResponse, ServerError> {
        debug!("fetching table from catalog");
        let table = self
            .catalog
            .get_table(client_id, share_name, schema_name, table_name)
            .await?;

        debug!("reading delta log");
        let table_data = self
            .reader
            .get_table_data(table.storage_path(), Version::Latest, None, None, None)
            .await?;

        let unsigned_actions = match capabilities.response_format() {
            ResponseFormat::Parquet => TableActionsResponse::new_parquet(table_data),
            ResponseFormat::Delta => TableActionsResponse::new_delta(table_data),
        };

        debug!("signing table actions");
        let signer = self.signers.get_or_noop("s3");
        let signed_actions = unsigned_actions.sign(table.storage_path(), signer).await;

        Ok(signed_actions)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        catalog::{CatalogError, MockCatalog},
        reader::MockTableReader,
    };
    use insta::assert_json_snapshot;
    use mockall::predicate::eq;

    #[tokio::test]
    async fn list_shares() {
        let mut mock_catalog = MockCatalog::new();
        mock_catalog.expect_list_shares().return_const(Ok(Page::new(
            vec![
                Share::builder()
                    .name("vaccine_share")
                    .id("edacc4a7-6600-4fbb-85f3-a62a5ce6761f")
                    .build()
                    .unwrap(),
                Share::builder()
                    .name("sales_share")
                    .id("3e979c79-6399-4dac-bcf8-54e268f48515")
                    .build()
                    .unwrap(),
            ],
            Some("continuation_token".to_owned()),
        )));
        let state = SharingServerState::new(
            Arc::new(mock_catalog),
            Arc::new(MockTableReader::new()),
            SignerRegistry::new(),
        );

        let response = state
            .list_shares(&RecipientId::Anonymous, &Pagination::default())
            .await
            .unwrap();

        assert_json_snapshot!(response);
    }

    #[tokio::test]
    async fn get_share() {
        let mut mock_catalog = MockCatalog::new();
        mock_catalog
            .expect_get_share()
            .return_const(Ok(Share::builder()
                .name("vaccine_share")
                .id("edacc4a7-6600-4fbb-85f3-a62a5ce6761f")
                .build()
                .unwrap()));

        let state = SharingServerState::new(
            Arc::new(mock_catalog),
            Arc::new(MockTableReader::new()),
            SignerRegistry::new(),
        );
        let response = state
            .get_share(&RecipientId::Anonymous, "vaccine_share")
            .await
            .unwrap();

        assert_json_snapshot!(response);
    }

    #[tokio::test]
    async fn get_share_not_found() {
        let mut mock_catalog = MockCatalog::new();
        mock_catalog
            .expect_get_share()
            .return_const(Err(CatalogError::not_found("share not found")));

        let state = SharingServerState::new(
            Arc::new(mock_catalog),
            Arc::new(MockTableReader::new()),
            SignerRegistry::new(),
        );

        let response = state
            .get_share(&RecipientId::Anonymous, "not-exising-share")
            .await
            .unwrap_err();

        assert_eq!(response, ServerError::not_found("share not found"));
    }

    #[tokio::test]
    async fn list_schemas() {
        let mut mock_catalog = MockCatalog::new();
        mock_catalog
            .expect_list_schemas()
            .return_const(Ok(Page::new(
                vec![Schema::builder()
                    .name("acme_vaccine_data")
                    .share_name("vaccine_share")
                    .build()
                    .unwrap()],
                Some("continuation_token".to_owned()),
            )));

        let state = SharingServerState::new(
            Arc::new(mock_catalog),
            Arc::new(MockTableReader::new()),
            SignerRegistry::new(),
        );
        let response = state
            .list_schemas(
                &RecipientId::Anonymous,
                "vaccine_share",
                &Pagination::default(),
            )
            .await
            .unwrap();

        assert_json_snapshot!(response);
    }

    #[tokio::test]
    async fn list_tables_in_share() {
        let mut mock_catalog = MockCatalog::new();
        mock_catalog
            .expect_list_tables_in_share()
            .return_const(Ok(Page::new(
                vec![Table::builder()
                    .name("vaccine_ingredients")
                    .schema_name("acme_vaccine_data")
                    .share_name("vaccine_share")
                    .id("dcb1e680-7da4-4041-9be8-88aff508d001")
                    .share_id("edacc4a7-6600-4fbb-85f3-a62a5ce6761f")
                    .storage_path("s3://bucket/prefix/key")
                    .build()
                    .unwrap()],
                Some("continuation_token".to_owned()),
            )));

        let state = SharingServerState::new(
            Arc::new(mock_catalog),
            Arc::new(MockTableReader::new()),
            SignerRegistry::new(),
        );
        let response = state
            .list_tables_in_share(
                &RecipientId::Anonymous,
                "vaccine_share",
                &Pagination::default(),
            )
            .await
            .unwrap();

        assert_json_snapshot!(response);
    }

    #[tokio::test]
    async fn list_tables_in_schema() {
        let mut mock_catalog = MockCatalog::new();
        mock_catalog
            .expect_list_tables_in_schema()
            .return_const(Ok(Page::new(
                vec![Table::builder()
                    .name("vaccine_ingredients")
                    .schema_name("acme_vaccine_data")
                    .share_name("vaccine_share")
                    .id("dcb1e680-7da4-4041-9be8-88aff508d001")
                    .share_id("edacc4a7-6600-4fbb-85f3-a62a5ce6761f")
                    .storage_path("s3://bucket/prefix/key")
                    .build()
                    .unwrap()],
                Some("continuation_token".to_owned()),
            )));

        let state = SharingServerState::new(
            Arc::new(mock_catalog),
            Arc::new(MockTableReader::new()),
            SignerRegistry::new(),
        );
        let response = state
            .list_tables_in_schema(
                &RecipientId::Anonymous,
                "vaccine_share",
                "acme_vaccine_data",
                &Pagination::default(),
            )
            .await
            .unwrap();

        assert_json_snapshot!(response);
    }

    #[tokio::test]
    async fn get_table_version() {
        let mut mock_catalog = MockCatalog::new();
        mock_catalog
            .expect_get_table()
            .return_const(Ok(Table::builder()
                .name("vaccine_ingredients")
                .schema_name("acme_vaccine_data")
                .share_name("vaccine_share")
                .id("dcb1e680-7da4-4041-9be8-88aff508d001")
                .share_id("edacc4a7-6600-4fbb-85f3-a62a5ce6761f")
                .storage_path("s3://bucket/prefix/key")
                .build()
                .unwrap()));

        let mut mock_reader = MockTableReader::new();
        mock_reader
            .expect_get_table_version_number()
            .with(eq("s3://bucket/prefix/key"), eq(Version::Latest))
            .once()
            .return_const(Ok(TableVersionNumber::new(17u64)));

        let state = SharingServerState::new(
            Arc::new(mock_catalog),
            Arc::new(mock_reader),
            SignerRegistry::new(),
        );
        let response = state
            .get_table_version_number(
                &RecipientId::Anonymous,
                "vaccine_share",
                "acme_vaccine_data",
                "vaccine_patients",
                Version::Latest,
            )
            .await
            .unwrap();

        assert_json_snapshot!(response);
    }

    #[tokio::test]
    async fn get_table_version_table_not_found() {
        let mut mock_catalog = MockCatalog::new();
        mock_catalog
            .expect_get_table()
            .return_const(Err(CatalogError::not_found("table not found")));

        let state = SharingServerState::new(
            Arc::new(mock_catalog),
            Arc::new(MockTableReader::new()),
            SignerRegistry::new(),
        );
        let response = state
            .get_table_version_number(
                &RecipientId::Anonymous,
                "vaccine_share",
                "acme_vaccine_data",
                "missing_table",
                Version::Latest,
            )
            .await
            .unwrap_err();

        assert_eq!(response, ServerError::not_found("table not found"))
    }

    // #[tokio::test]
    // async fn get_table_version_internal_error() {
    //     let mut mock_table_manager = Catalog::new();
    //     mock_table_manager
    //         .expect_get_table()
    //         .with(
    //             eq("vaccine_share"),
    //             eq("acme_vaccine_data"),
    //             eq("vaccine_patients"),
    //         )
    //         .once()
    //         .return_const(Err(CatalogError::Other {
    //             reason: "something went wrong internally".to_owned(),
    //         }));

    //     let state = SharingServerState::new(Arc::new(mock_table_manager));

    //     let response = state
    //         .get_table_version(
    //             "vaccine_share",
    //             "acme_vaccine_data",
    //             "vaccine_patients",
    //             Version::Latest,
    //         )
    //         .await;
    //     assert!(response.is_err());
    //     assert_eq!(
    //         response.unwrap_err(),
    //         ServerError::ShareManagerError {
    //             reason: String::from("something went wrong internally")
    //         }
    //     )
    // }

    // #[tokio::test]
    // async fn get_table_metadata() {
    //     let mut mock_table_manager = Catalog::new();
    //     mock_table_manager
    //         .expect_get_table()
    //         .with(
    //             eq("vaccine_share"),
    //             eq("acme_vaccine_data"),
    //             eq("vaccine_patients"),
    //         )
    //         .once()
    //         .returning(|_, _, _| {
    //             let share = ShareBuilder::new("vaccine_share")
    //                 .id("edacc4a7-6600-4fbb-85f3-a62a5ce6761f")
    //                 .build();
    //             let schema = SchemaBuilder::new(share, "acme_vaccine_data").build();
    //             Ok(TableBuilder::new(
    //                 schema,
    //                 "vaccine_patients",
    //                 "s3://vaccine_share/acme_vaccine_data/vaccine_patients".to_owned(),
    //             )
    //             .id("c48f3e19-2c29-4ea3-b6f7-3899e53338fa")
    //             .build())
    //         });

    //     let mut mock_delta_reader = MockTableReader::new();

    //     let table_metadata = MetadataBuilder::new("f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2", "{\"type\":\"struct\",\"fields\":[{\"name\":\"eventTime\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}").partition_columns(vec!["date".to_owned()]).build();

    //     mock_delta_reader
    //         .expect_get_table_metadata()
    //         .with(eq("s3://vaccine_share/acme_vaccine_data/vaccine_patients"))
    //         .once()
    //         .return_const(Ok(TableMetadata {
    //             version: 123u64,
    //             protocol: ProtocolBuilder::new().build(),
    //             metadata: table_metadata,
    //         }));

    //     let mut state = SharingServerState::new(Arc::new(mock_table_manager));
    //     state.add_table_reader("DELTA", Arc::new(mock_delta_reader));

    //     let response = state
    //         .get_table_metadata("vaccine_share", "acme_vaccine_data", "vaccine_patients")
    //         .await
    //         .unwrap();
    //     assert_json_snapshot!(response);
    // }

    // #[tokio::test]
    // async fn get_table_data() {
    //     let mut mock_table_manager = Catalog::new();
    //     mock_table_manager
    //         .expect_get_table()
    //         .with(
    //             eq("vaccine_share"),
    //             eq("acme_vaccine_data"),
    //             eq("vaccine_patients"),
    //         )
    //         .once()
    //         .returning(|_, _, _| {
    //             let share = ShareBuilder::new("vaccine_share")
    //                 .id("edacc4a7-6600-4fbb-85f3-a62a5ce6761f")
    //                 .build();
    //             let schema = SchemaBuilder::new(share, "acme_vaccine_data").build();
    //             Ok(TableBuilder::new(
    //                 schema,
    //                 "vaccine_patients",
    //                 "s3://vaccine_share/acme_vaccine_data/vaccine_patients",
    //             )
    //             .id("c48f3e19-2c29-4ea3-b6f7-3899e53338fa")
    //             .build())
    //         });

    //     let table_metadata = MetadataBuilder::new("f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2", "{\"type\":\"struct\",\"fields\":[{\"name\":\"eventTime\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}").partition_columns(vec!["date".to_owned()]).build();

    //     let mut mock_delta_reader = MockTableReader::new();
    //     mock_delta_reader
    //         .expect_get_table_data()
    //         .with(
    //             eq("s3://vaccine_share/acme_vaccine_data/vaccine_patients"),
    //             eq(0u64),
    //             eq(None),
    //             eq(None),
    //         )
    //         .once()
    //         .return_const(Ok(UnsignedTableData {
    //             version: 123u64,
    //             protocol: ProtocolBuilder::new().build(),
    //             metadata: table_metadata,
    //             data: vec![
    //                 FileBuilder::new(
    //                     "https://test-bucket.s3.eu-west-1.amazonaws.com/file1",
    //                     "8b0086f2-7b27-4935-ac5a-8ed6215a6640",
    //                 )
    //                 .build()
    //                 .into(),
    //                 FileBuilder::new(
    //                     "https://test-bucket.s3.eu-west-1.amazonaws.com/file2",
    //                     "591723a8-6a27-4240-a90e-57426f4736d2",
    //                 )
    //                 .build()
    //                 .into(),
    //             ],
    //         }));

    //     let table_metadata = MetadataBuilder::new("f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2", "{\"type\":\"struct\",\"fields\":[{\"name\":\"eventTime\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}").partition_columns(vec!["date".to_owned()]).build();

    //     let mut mock_url_signer = MockUrlSigner::new();
    //     mock_url_signer
    //         .expect_sign_table_data()
    //         .times(1)
    //         .return_const(SignedTableData {
    //             version: 123u64,
    //             protocol: ProtocolBuilder::new().build(),
    //             metadata: table_metadata,
    //             data: vec![
    //                 SignedDataFile::File(
    //                     FileBuilder::new(
    //                         "https://test-bucket.s3.eu-west-1.amazonaws.com/file1?signature=123",
    //                         "8b0086f2-7b27-4935-ac5a-8ed6215a6640",
    //                     )
    //                     .build(),
    //                 ),
    //                 SignedDataFile::File(
    //                     FileBuilder::new(
    //                         "https://test-bucket.s3.eu-west-1.amazonaws.com/file2?signature=123",
    //                         "591723a8-6a27-4240-a90e-57426f4736d2",
    //                     )
    //                     .build(),
    //                 ),
    //             ],
    //         });

    //     let mut state = SharingServerState::new(Arc::new(mock_table_manager));
    //     state.add_table_reader("DELTA", Arc::new(mock_delta_reader));
    //     state.add_url_signer("S3", Arc::new(mock_url_signer));

    //     let response = state
    //         .get_table_data(
    //             "vaccine_share",
    //             "acme_vaccine_data",
    //             "vaccine_patients",
    //             Version::Latest,
    //         )
    //         .await
    //         .unwrap();
    //     assert_json_snapshot!(response);
    // }
}
