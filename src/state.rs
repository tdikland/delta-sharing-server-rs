//! Delta Sharing server state.

use std::{collections::HashMap, sync::Arc};

use crate::{
    auth::ClientId,
    error::ServerError,
    protocol::table::{SignedTableData, TableMetadata, TableVersionNumber},
    reader::{TableReader, Version},
    share_reader::{Page, Pagination, Schema, Share, ShareReader, Table},
    signer::UrlSigner,
};

/// State of the sharing server.
#[derive(Clone)]
pub struct SharingServerState {
    catalog: Arc<dyn ShareReader>,
    reader: Arc<dyn TableReader>,
    url_signers: HashMap<String, Arc<dyn UrlSigner>>,
}

impl SharingServerState {
    /// Create a new sharing server state.
    pub fn new(catalog: Arc<dyn ShareReader>, reader: Arc<dyn TableReader>) -> Self {
        Self {
            catalog,
            reader,
            url_signers: HashMap::new(),
        }
    }

    /// Get the catalog from the state.
    pub fn catalog(&self) -> Arc<dyn ShareReader> {
        self.catalog.clone()
    }

    /// Get the reader from the state.
    pub fn reader(&self) -> Arc<dyn TableReader> {
        self.reader.clone()
    }

    /// Add a url signer to the state.
    pub fn add_url_signer(&mut self, storage: impl Into<String>, signer: Arc<dyn UrlSigner>) {
        self.url_signers.insert(storage.into(), signer);
    }

    /// Get the url signer for a specific object store.
    pub fn url_signer(&self, storage: &str) -> Option<Arc<dyn UrlSigner>> {
        self.url_signers.get(storage).cloned()
    }

    /// Get a list of shares in the share store.
    pub async fn list_shares(
        &self,
        client_id: &ClientId,
        pagination: &Pagination,
    ) -> Result<Page<Share>, ServerError> {
        self.catalog
            .list_shares(client_id, pagination)
            .await
            .map_err(Into::into)
    }

    /// Get a share from the share store.
    pub async fn get_share(
        &self,
        client_id: &ClientId,
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
        client_id: &ClientId,
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
        client_id: &ClientId,
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
        client_id: &ClientId,
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
        client_id: &ClientId,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
        version: Version,
    ) -> Result<TableVersionNumber, ServerError> {
        let table = self
            .catalog
            .get_table(client_id, share_name, schema_name, table_name)
            .await?;

        let table_version = self
            .reader
            .get_table_version_number(table.storage_path(), version)
            .await?;

        Ok(table_version)
    }

    /// Get the metadata of a table.
    pub async fn get_table_metadata(
        &self,
        client_id: &ClientId,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<TableMetadata, ServerError> {
        let table = self
            .catalog
            .get_table(client_id, share_name, schema_name, table_name)
            .await?;

        let metadata = self.reader.get_table_metadata(table.storage_path()).await?;

        Ok(metadata)
    }

    /// Get the data files of a table version.
    pub async fn get_table_data(
        &self,
        client_id: &ClientId,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
        _version: Version,
    ) -> Result<SignedTableData, ServerError> {
        let table = self
            .catalog
            .get_table(client_id, share_name, schema_name, table_name)
            .await?;

        let table_data = self
            .reader
            .get_table_data(table.storage_path(), 0, None, None)
            .await?;

        let signer = self
            .url_signer("S3")
            .ok_or(ServerError::UnsupportedTableStorage {
                storage: String::from("S3"),
            })?;

        let signed_table_data = signer.sign_table_data(table_data).await;
        Ok(signed_table_data)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{reader::MockTableReader, share_reader::MockShareReader};
    use insta::assert_json_snapshot;

    #[tokio::test]
    async fn list_shares() {
        let mut mock_table_manager = MockShareReader::new();
        mock_table_manager
            .expect_list_shares()
            .once()
            .returning(|_, _| {
                let shares = Page::new(
                    vec![
                        Share::new(
                            "vaccine_share".to_owned(),
                            Some("edacc4a7-6600-4fbb-85f3-a62a5ce6761f".to_owned()),
                        ),
                        Share::new(
                            "sales_share".to_owned(),
                            Some("3e979c79-6399-4dac-bcf8-54e268f48515".to_owned()),
                        ),
                    ],
                    Some("continuation_token".to_owned()),
                );
                Ok(shares)
            });
        let mock_reader = MockTableReader::new();

        let state = SharingServerState::new(Arc::new(mock_table_manager), Arc::new(mock_reader));
        let response = state
            .list_shares(&ClientId::Anonymous, &Pagination::default())
            .await
            .unwrap();
        assert_json_snapshot!(response);
    }

    // #[tokio::test]
    // async fn list_shares_with_pagination() {
    //     let mut mock_table_manager = Catalog::new();
    //     mock_table_manager
    //         .expect_list_shares()
    //         .with(eq(ListCursor::new(None, None)))
    //         .returning(|_| {
    //             let mut shares = List::new(vec![], Some("continuation_token".to_owned()));
    //             let share = ShareBuilder::new("vaccine_share")
    //                 .id("edacc4a7-6600-4fbb-85f3-a62a5ce6761f")
    //                 .build();
    //             shares.push(share);
    //             Ok(shares)
    //         });

    //     mock_table_manager
    //         .expect_list_shares()
    //         .with(eq(ListCursor::new(
    //             None,
    //             Some("continuation_token".to_owned()),
    //         )))
    //         .returning(|_| {
    //             let mut shares = List::new(vec![], None);
    //             shares.push(
    //                 ShareBuilder::new("sales_share")
    //                     .id("3e979c79-6399-4dac-bcf8-54e268f48515")
    //                     .build(),
    //             );
    //             Ok(shares)
    //         });

    //     let state = SharingServerState::new(Arc::new(mock_table_manager));
    //     let response1 = state.list_shares(&ListCursor::default()).await.unwrap();
    //     assert_json_snapshot!(response1);

    //     let response2 = state
    //         .list_shares(&ListCursor::new(
    //             None,
    //             Some("continuation_token".to_owned()),
    //         ))
    //         .await
    //         .unwrap();
    //     assert_json_snapshot!(response2);
    // }

    // #[tokio::test]
    // async fn list_shares_malformed_token() {
    //     let mut mock_table_manager = Catalog::new();
    //     mock_table_manager
    //         .expect_list_shares()
    //         .once()
    //         .returning(|_| Err(CatalogError::MalformedContinuationToken));

    //     let state = SharingServerState::new(Arc::new(mock_table_manager));
    //     let response = state
    //         .list_shares(&ListCursor::new(None, Some("invalid_token".to_owned())))
    //         .await;
    //     assert!(response.is_err());
    //     assert_eq!(
    //         response.unwrap_err(),
    //         ServerError::InvalidPaginationToken {
    //             reason: String::from("the provided `page_token` is malformed")
    //         }
    //     );
    // }

    // #[tokio::test]
    // async fn get_share() {
    //     let mut mock_table_manager = Catalog::new();
    //     mock_table_manager
    //         .expect_get_share()
    //         .with(eq("vaccine_share"))
    //         .once()
    //         .returning(|_| {
    //             Ok(ShareBuilder::new("vaccine_share")
    //                 .id("edacc4a7-6600-4fbb-85f3-a62a5ce6761f")
    //                 .build())
    //         });

    //     let state = SharingServerState::new(Arc::new(mock_table_manager));
    //     let response = state.get_share("vaccine_share").await.unwrap();
    //     assert_json_snapshot!(response);
    // }

    // #[tokio::test]
    // async fn get_share_not_found() {
    //     let mut mock_table_manager = Catalog::new();
    //     mock_table_manager
    //         .expect_get_share()
    //         .with(eq("vaccine_share"))
    //         .once()
    //         .returning(|_| {
    //             Err(CatalogError::ShareNotFound {
    //                 share_name: "vaccine_share".to_owned(),
    //             })
    //         });

    //     let state = SharingServerState::new(Arc::new(mock_table_manager));
    //     let response = state.get_share("vaccine_share").await;
    //     assert!(response.is_err());
    //     assert_eq!(
    //         response.unwrap_err(),
    //         ServerError::ShareNotFound {
    //             name: "vaccine_share".to_owned()
    //         }
    //     );
    // }

    // #[tokio::test]
    // async fn list_schemas() {
    //     let mut mock_table_manager = Catalog::new();
    //     mock_table_manager
    //         .expect_list_schemas()
    //         .with(eq("vaccine_share"), eq(ListCursor::default()))
    //         .once()
    //         .returning(|_, _| {
    //             let mut schemas = List::new(vec![], Some("continuation_token".to_owned()));
    //             let share = ShareBuilder::new("vaccine_share").build();
    //             let schema = SchemaBuilder::new(share, "acme_vaccine_data").build();
    //             schemas.push(schema);
    //             Ok(schemas)
    //         });

    //     let state = SharingServerState::new(Arc::new(mock_table_manager));
    //     let response = state
    //         .list_schemas("vaccine_share", &ListCursor::default())
    //         .await
    //         .unwrap();
    //     assert_json_snapshot!(response);
    // }

    // #[tokio::test]
    // async fn list_tables_in_share() {
    //     let mut mock_table_manager = Catalog::new();
    //     mock_table_manager
    //         .expect_list_tables_in_share()
    //         .with(eq("vaccine_share"), eq(ListCursor::default()))
    //         .once()
    //         .returning(|_, _| {
    //             let mut tables = List::new(vec![], Some("next_page_token".to_owned()));
    //             let share = ShareBuilder::new("vaccine_share")
    //                 .id("edacc4a7-6600-4fbb-85f3-a62a5ce6761f")
    //                 .build();
    //             let schema = SchemaBuilder::new(share, "acme_vaccine_data").build();
    //             tables.push(
    //                 TableBuilder::new(
    //                     schema.clone(),
    //                     "vaccine_ingredients",
    //                     "s3://vaccine_share/acme_vaccine_data/vaccine_ingredients",
    //                 )
    //                 .id("dcb1e680-7da4-4041-9be8-88aff508d001")
    //                 .build(),
    //             );
    //             tables.push(
    //                 TableBuilder::new(
    //                     schema,
    //                     "vaccine_patients",
    //                     "s3://vaccine_share/acme_vaccine_data/vaccine_patients",
    //                 )
    //                 .id("c48f3e19-2c29-4ea3-b6f7-3899e53338fa")
    //                 .build(),
    //             );
    //             Ok(tables)
    //         });

    //     let state = SharingServerState::new(Arc::new(mock_table_manager));
    //     let response = state
    //         .list_tables_in_share("vaccine_share", &ListCursor::default())
    //         .await
    //         .unwrap();
    //     assert_json_snapshot!(response);
    // }

    // #[tokio::test]
    // async fn list_tables_in_schema() {
    //     let mut mock_table_manager = Catalog::new();
    //     mock_table_manager
    //         .expect_list_tables_in_schema()
    //         .with(
    //             eq("vaccine_share"),
    //             eq("acme_vaccine_data"),
    //             eq(ListCursor::default()),
    //         )
    //         .once()
    //         .returning(|_, _, _| {
    //             let mut tables = List::new(vec![], Some("next_page_token".to_owned()));
    //             let share = ShareBuilder::new("vaccine_share")
    //                 .id("edacc4a7-6600-4fbb-85f3-a62a5ce6761f")
    //                 .build();
    //             let schema = SchemaBuilder::new(share, "acme_vaccine_data").build();
    //             tables.push(
    //                 TableBuilder::new(
    //                     schema.clone(),
    //                     "vaccine_ingredients",
    //                     "s3://vaccine_share/acme_vaccine_data/vaccine_ingredients",
    //                 )
    //                 .id("dcb1e680-7da4-4041-9be8-88aff508d001")
    //                 .build(),
    //             );
    //             tables.push(
    //                 TableBuilder::new(
    //                     schema,
    //                     "vaccine_patients",
    //                     "s3://vaccine_share/acme_vaccine_data/vaccine_patients",
    //                 )
    //                 .id("c48f3e19-2c29-4ea3-b6f7-3899e53338fa")
    //                 .build(),
    //             );
    //             Ok(tables)
    //         });

    //     let state = SharingServerState::new(Arc::new(mock_table_manager));
    //     let response = state
    //         .list_tables_in_schema("vaccine_share", "acme_vaccine_data", &ListCursor::default())
    //         .await
    //         .unwrap();
    //     assert_json_snapshot!(response);
    // }

    // #[tokio::test]
    // async fn get_table_version() {
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
    //             let table = TableBuilder::new(
    //                 schema,
    //                 "vaccine_patients",
    //                 "s3://vaccine_share/acme_vaccine_data/vaccine_patients",
    //             )
    //             .id("c48f3e19-2c29-4ea3-b6f7-3899e53338fa")
    //             .build();
    //             Ok(table)
    //         });

    //     let mut mock_delta_reader = MockTableReader::new();
    //     mock_delta_reader
    //         .expect_get_table_version()
    //         .with(
    //             eq("s3://vaccine_share/acme_vaccine_data/vaccine_patients"),
    //             eq(Version::Latest),
    //         )
    //         .once()
    //         .return_const(Ok(17u64));

    //     let mut state = SharingServerState::new(Arc::new(mock_table_manager));
    //     state.add_table_reader("DELTA", Arc::new(mock_delta_reader));

    //     let response = state
    //         .get_table_version(
    //             "vaccine_share",
    //             "acme_vaccine_data",
    //             "vaccine_patients",
    //             Version::Latest,
    //         )
    //         .await
    //         .unwrap();
    //     assert_json_snapshot!(response);
    // }

    // #[tokio::test]
    // async fn get_table_version_table_not_found() {
    //     let mut mock_table_manager = Catalog::new();
    //     mock_table_manager
    //         .expect_get_table()
    //         .with(
    //             eq("vaccine_share"),
    //             eq("acme_vaccine_data"),
    //             eq("missing_table"),
    //         )
    //         .once()
    //         .return_const(Err(CatalogError::TableNotFound {
    //             share_name: "vaccine_share".to_owned(),
    //             schema_name: "acme_vaccine_data".to_owned(),
    //             table_name: "missing_table".to_owned(),
    //         }));

    //     let state = SharingServerState::new(Arc::new(mock_table_manager));

    //     let response = state
    //         .get_table_version(
    //             "vaccine_share",
    //             "acme_vaccine_data",
    //             "missing_table",
    //             Version::Latest,
    //         )
    //         .await;
    //     assert!(response.is_err());
    //     assert_eq!(
    //         response.unwrap_err(),
    //         ServerError::TableNotFound {
    //             name: "vaccine_share.acme_vaccine_data.missing_table".to_owned()
    //         }
    //     )
    // }

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
