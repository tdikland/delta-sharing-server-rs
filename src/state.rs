//! Delta Sharing server state.

use std::{collections::HashMap, sync::Arc};

use crate::{
    error::ServerError,
    manager::ShareReader,
    protocol::{share::ListCursor, table::Version},
    reader::TableReader,
    response::{
        GetShareResponse, ListSchemasResponse, ListSharesResponse, ListTablesResponse,
        TableInfoResponse, TableVersionResponse,
    },
    signer::UrlSigner,
};

/// State of the sharing server.
#[derive(Clone)]
pub struct SharingServerState {
    shared_table_manager: Arc<dyn ShareReader>,
    table_readers: HashMap<String, Arc<dyn TableReader>>,
    url_signers: HashMap<String, Arc<dyn UrlSigner>>,
}

impl SharingServerState {
    /// Create a new sharing server state.
    pub fn new(manager: Arc<dyn ShareReader>) -> Self {
        Self {
            shared_table_manager: manager,
            table_readers: HashMap::new(),
            url_signers: HashMap::new(),
        }
    }

    /// Add a table reader to the state.
    pub fn add_table_reader(&mut self, format: impl Into<String>, reader: Arc<dyn TableReader>) {
        self.table_readers.insert(format.into(), reader);
    }

    /// Add a url signer to the state.
    pub fn add_url_signer(&mut self, storage: impl Into<String>, signer: Arc<dyn UrlSigner>) {
        self.url_signers.insert(storage.into(), signer);
    }

    /// Set the table readers.
    pub fn set_table_readers(&mut self, readers: HashMap<String, Arc<dyn TableReader>>) {
        self.table_readers = readers;
    }

    /// Set the url signers.
    pub fn set_url_signers(&mut self, signers: HashMap<String, Arc<dyn UrlSigner>>) {
        self.url_signers = signers;
    }

    /// Get the share rearder.
    pub fn table_manager(&self) -> Arc<dyn ShareReader> {
        self.shared_table_manager.clone()
    }

    /// Get the table reader for a specific format.
    pub fn table_reader(&self, format: &str) -> Option<Arc<dyn TableReader>> {
        self.table_readers.get(format).cloned()
    }

    /// Get the url signer for a specific object store.
    pub fn url_signer(&self, storage: &str) -> Option<Arc<dyn UrlSigner>> {
        self.url_signers.get(storage).cloned()
    }

    /// Get a list of shares in the share store.
    pub async fn list_shares(
        &self,
        cursor: &ListCursor,
    ) -> Result<ListSharesResponse, ServerError> {
        let shares = self.shared_table_manager.list_shares(&cursor).await?;
        Ok(shares.into())
    }

    /// Get a share from the share store.
    pub async fn get_share(&self, share_name: &str) -> Result<GetShareResponse, ServerError> {
        let share = self.shared_table_manager.get_share(share_name).await?;
        Ok(share.into())
    }

    /// Get a list of schemas in a share.
    pub async fn list_schemas(
        &self,
        share_name: &str,
        cursor: &ListCursor,
    ) -> Result<ListSchemasResponse, ServerError> {
        let schemas = self
            .shared_table_manager
            .list_schemas(share_name, &cursor)
            .await?;
        Ok(schemas.into())
    }

    /// Get a list of tables in a share.
    pub async fn list_tables_in_share(
        &self,
        share_name: &str,
        cursor: &ListCursor,
    ) -> Result<ListTablesResponse, ServerError> {
        let tables = self
            .shared_table_manager
            .list_tables_in_share(share_name, &cursor)
            .await?;
        Ok(tables.into())
    }

    /// Get a list of tables in a schema.
    pub async fn list_tables_in_schema(
        &self,
        share_name: &str,
        schema_name: &str,
        cursor: &ListCursor,
    ) -> Result<ListTablesResponse, ServerError> {
        let tables = self
            .shared_table_manager
            .list_tables_in_schema(share_name, schema_name, &cursor)
            .await?;
        Ok(tables.into())
    }

    /// Get the version of a table.
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

    /// Get the metadata of a table.
    pub async fn get_table_metadata(
        &self,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<TableInfoResponse, ServerError> {
        let table = self
            .table_manager()
            .get_table(&share_name, &schema_name, &table_name)
            .await?;

        let table_metadata = self
            .table_reader(table.format())
            .ok_or(ServerError::UnsupportedTableFormat {
                format: table.format().to_owned(),
            })?
            .get_table_metadata(table.storage_path())
            .await?;

        Ok(table_metadata.into())
    }

    /// Get the data files of a table version.
    pub async fn get_table_data(
        &self,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
        _version: Version,
    ) -> Result<TableInfoResponse, ServerError> {
        let table = self
            .table_manager()
            .get_table(&share_name, &schema_name, &table_name)
            .await?;

        let table_data = self
            .table_reader(table.format())
            .ok_or(ServerError::UnsupportedTableFormat {
                format: table.format().to_owned(),
            })?
            .get_table_data(table.storage_path(), 0, None, None)
            .await?;

        let signer = self
            .url_signer("S3")
            .ok_or(ServerError::UnsupportedTableStorage {
                storage: String::from("S3"),
            })?;

        let signed_table_data = signer.sign_table_data(table_data).await;
        Ok(signed_table_data.into())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        manager::{MockShareReader, ShareReaderError},
        protocol::{
            action::{File, FileFormat, Metadata, Protocol},
            securable::{Schema, Share, Table},
            share::List,
            table::{
                SignedDataFile, SignedTableData, TableMetadata, UnsignedDataFile, UnsignedTableData,
            },
        },
        reader::MockTableReader,
        signer::MockUrlSigner,
    };
    use insta::assert_json_snapshot;
    use mockall::predicate::eq;

    #[tokio::test]
    async fn list_shares() {
        let mut mock_table_manager = MockShareReader::new();
        mock_table_manager
            .expect_list_shares()
            .once()
            .returning(|_| {
                let mut shares = List::new(vec![], Some("continuation_token".to_owned()));
                shares.push(Share::new(
                    "vaccine_share".to_owned(),
                    Some("edacc4a7-6600-4fbb-85f3-a62a5ce6761f".to_owned()),
                ));
                shares.push(Share::new(
                    "sales_share".to_owned(),
                    Some("3e979c79-6399-4dac-bcf8-54e268f48515".to_owned()),
                ));
                Ok(shares)
            });

        let state = SharingServerState::new(Arc::new(mock_table_manager));
        let response = state.list_shares(&ListCursor::default()).await.unwrap();
        assert_json_snapshot!(response);
    }

    #[tokio::test]
    async fn list_shares_with_pagination() {
        let mut mock_table_manager = MockShareReader::new();
        mock_table_manager
            .expect_list_shares()
            .with(eq(ListCursor::new(None, None)))
            .returning(|_| {
                let mut shares = List::new(vec![], Some("continuation_token".to_owned()));
                let share = Share::new(
                    "vaccine_share".to_owned(),
                    Some("edacc4a7-6600-4fbb-85f3-a62a5ce6761f".to_owned()),
                );
                shares.push(share);
                Ok(shares)
            });

        mock_table_manager
            .expect_list_shares()
            .with(eq(ListCursor::new(
                None,
                Some("continuation_token".to_owned()),
            )))
            .returning(|_| {
                let mut shares = List::new(vec![], None);
                shares.push(Share::new(
                    "sales_share".to_owned(),
                    Some("3e979c79-6399-4dac-bcf8-54e268f48515".to_owned()),
                ));
                Ok(shares)
            });

        let state = SharingServerState::new(Arc::new(mock_table_manager));
        let response1 = state.list_shares(&ListCursor::default()).await.unwrap();
        assert_json_snapshot!(response1);

        let response2 = state
            .list_shares(&ListCursor::new(
                None,
                Some("continuation_token".to_owned()),
            ))
            .await
            .unwrap();
        assert_json_snapshot!(response2);
    }

    #[tokio::test]
    async fn list_shares_malformed_token() {
        let mut mock_table_manager = MockShareReader::new();
        mock_table_manager
            .expect_list_shares()
            .once()
            .returning(|_| Err(ShareReaderError::MalformedContinuationToken));

        let state = SharingServerState::new(Arc::new(mock_table_manager));
        let response = state
            .list_shares(&ListCursor::new(None, Some("invalid_token".to_owned())))
            .await;
        assert!(response.is_err());
        assert_eq!(response.unwrap_err(), ServerError::MalformedNextPageToken);
    }

    #[tokio::test]
    async fn get_share() {
        let mut mock_table_manager = MockShareReader::new();
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
    async fn get_share_not_found() {
        let mut mock_table_manager = MockShareReader::new();
        mock_table_manager
            .expect_get_share()
            .with(eq("vaccine_share"))
            .once()
            .returning(|_| {
                Err(ShareReaderError::ShareNotFound {
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
    async fn list_schemas() {
        let mut mock_table_manager = MockShareReader::new();
        mock_table_manager
            .expect_list_schemas()
            .with(eq("vaccine_share"), eq(ListCursor::default()))
            .once()
            .returning(|_, _| {
                let mut schemas = List::new(vec![], Some("continuation_token".to_owned()));
                let share = Share::new("vaccine_share".to_owned(), None);
                let schema = Schema::new(share, "acme_vaccine_data".to_owned(), None);
                schemas.push(schema);
                Ok(schemas)
            });

        let state = SharingServerState::new(Arc::new(mock_table_manager));
        let response = state
            .list_schemas("vaccine_share", &ListCursor::default())
            .await
            .unwrap();
        assert_json_snapshot!(response);
    }

    #[tokio::test]
    async fn list_tables_in_share() {
        let mut mock_table_manager = MockShareReader::new();
        mock_table_manager
            .expect_list_tables_in_share()
            .with(eq("vaccine_share"), eq(ListCursor::default()))
            .once()
            .returning(|_, _| {
                let mut tables = List::new(vec![], Some("next_page_token".to_owned()));
                let share = Share::new(
                    "vaccine_share".to_owned(),
                    Some("edacc4a7-6600-4fbb-85f3-a62a5ce6761f".to_owned()),
                );
                let schema = Schema::new(share, "acme_vaccine_data".to_owned(), None);
                tables.push(Table::new(
                    schema.clone(),
                    "vaccine_ingredients".to_owned(),
                    Some("dcb1e680-7da4-4041-9be8-88aff508d001".to_owned()),
                    "s3://vaccine_share/acme_vaccine_data/vaccine_ingredients".to_owned(),
                    None,
                ));
                tables.push(Table::new(
                    schema.clone(),
                    "vaccine_patients".to_owned(),
                    Some("c48f3e19-2c29-4ea3-b6f7-3899e53338fa".to_owned()),
                    "s3://vaccine_share/acme_vaccine_data/vaccine_patients".to_owned(),
                    None,
                ));
                Ok(tables)
            });

        let state = SharingServerState::new(Arc::new(mock_table_manager));
        let response = state
            .list_tables_in_share("vaccine_share", &ListCursor::default())
            .await
            .unwrap();
        assert_json_snapshot!(response);
    }

    #[tokio::test]
    async fn list_tables_in_schema() {
        let mut mock_table_manager = MockShareReader::new();
        mock_table_manager
            .expect_list_tables_in_schema()
            .with(
                eq("vaccine_share"),
                eq("acme_vaccine_data"),
                eq(ListCursor::default()),
            )
            .once()
            .returning(|_, _, _| {
                let mut tables = List::new(vec![], Some("next_page_token".to_owned()));
                let share = Share::new(
                    "vaccine_share".to_owned(),
                    Some("edacc4a7-6600-4fbb-85f3-a62a5ce6761f".to_owned()),
                );
                let schema = Schema::new(share, "acme_vaccine_data".to_owned(), None);
                tables.push(Table::new(
                    schema.clone(),
                    "vaccine_ingredients".to_owned(),
                    Some("dcb1e680-7da4-4041-9be8-88aff508d001".to_owned()),
                    "s3://vaccine_share/acme_vaccine_data/vaccine_ingredients".to_owned(),
                    None,
                ));
                tables.push(Table::new(
                    schema.clone(),
                    "vaccine_patients".to_owned(),
                    Some("c48f3e19-2c29-4ea3-b6f7-3899e53338fa".to_owned()),
                    "s3://vaccine_share/acme_vaccine_data/vaccine_patients".to_owned(),
                    None,
                ));
                Ok(tables)
            });

        let state = SharingServerState::new(Arc::new(mock_table_manager));
        let response = state
            .list_tables_in_schema("vaccine_share", "acme_vaccine_data", &ListCursor::default())
            .await
            .unwrap();
        assert_json_snapshot!(response);
    }

    #[tokio::test]
    async fn get_table_version() {
        let mut mock_table_manager = MockShareReader::new();
        mock_table_manager
            .expect_get_table()
            .with(
                eq("vaccine_share"),
                eq("acme_vaccine_data"),
                eq("vaccine_patients"),
            )
            .once()
            .returning(|_, _, _| {
                let share = Share::new(
                    "vaccine_share".to_owned(),
                    Some("edacc4a7-6600-4fbb-85f3-a62a5ce6761f".to_owned()),
                );
                let schema = Schema::new(share, "acme_vaccine_data".to_owned(), None);
                Ok(Table::new(
                    schema,
                    "vaccine_patients".to_owned(),
                    Some("c48f3e19-2c29-4ea3-b6f7-3899e53338fa".to_owned()),
                    "s3://vaccine_share/acme_vaccine_data/vaccine_patients".to_owned(),
                    Some("DELTA".to_owned()),
                ))
            });

        let mut mock_delta_reader = MockTableReader::new();
        mock_delta_reader
            .expect_get_table_version()
            .with(
                eq("s3://vaccine_share/acme_vaccine_data/vaccine_patients"),
                eq(Version::Latest),
            )
            .once()
            .return_const(Ok(17u64));

        let mut state = SharingServerState::new(Arc::new(mock_table_manager));
        state.add_table_reader("DELTA", Arc::new(mock_delta_reader));

        let response = state
            .get_table_version(
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
        let mut mock_table_manager = MockShareReader::new();
        mock_table_manager
            .expect_get_table()
            .with(
                eq("vaccine_share"),
                eq("acme_vaccine_data"),
                eq("missing_table"),
            )
            .once()
            .return_const(Err(ShareReaderError::TableNotFound {
                share_name: "vaccine_share".to_owned(),
                schema_name: "acme_vaccine_data".to_owned(),
                table_name: "missing_table".to_owned(),
            }));

        let state = SharingServerState::new(Arc::new(mock_table_manager));

        let response = state
            .get_table_version(
                "vaccine_share",
                "acme_vaccine_data",
                "missing_table",
                Version::Latest,
            )
            .await;
        assert!(response.is_err());
        assert_eq!(
            response.unwrap_err(),
            ServerError::TableNotFound {
                name: "vaccine_share.acme_vaccine_data.missing_table".to_owned()
            }
        )
    }

    #[tokio::test]
    async fn get_table_version_internal_error() {
        let mut mock_table_manager = MockShareReader::new();
        mock_table_manager
            .expect_get_table()
            .with(
                eq("vaccine_share"),
                eq("acme_vaccine_data"),
                eq("vaccine_patients"),
            )
            .once()
            .return_const(Err(ShareReaderError::Other {
                reason: "something went wrong internally".to_owned(),
            }));

        let state = SharingServerState::new(Arc::new(mock_table_manager));

        let response = state
            .get_table_version(
                "vaccine_share",
                "acme_vaccine_data",
                "vaccine_patients",
                Version::Latest,
            )
            .await;
        assert!(response.is_err());
        assert_eq!(response.unwrap_err(), ServerError::Other)
    }

    #[tokio::test]
    async fn get_table_metadata() {
        let mut mock_table_manager = MockShareReader::new();
        mock_table_manager
            .expect_get_table()
            .with(
                eq("vaccine_share"),
                eq("acme_vaccine_data"),
                eq("vaccine_patients"),
            )
            .once()
            .returning(|_, _, _| {
                let share = Share::new(
                    "vaccine_share".to_owned(),
                    Some("edacc4a7-6600-4fbb-85f3-a62a5ce6761f".to_owned()),
                );
                let schema = Schema::new(share, "acme_vaccine_data".to_owned(), None);
                Ok(Table::new(
                    schema,
                    "vaccine_patients".to_owned(),
                    Some("c48f3e19-2c29-4ea3-b6f7-3899e53338fa".to_owned()),
                    "s3://vaccine_share/acme_vaccine_data/vaccine_patients".to_owned(),
                    Some("DELTA".to_owned()),
                ))
            });

        let mut mock_delta_reader = MockTableReader::new();
        mock_delta_reader
            .expect_get_table_metadata()
            .with(eq("s3://vaccine_share/acme_vaccine_data/vaccine_patients"))
            .once()
            .return_const(Ok(TableMetadata {
                version: 123u64,
                protocol: Protocol {
                    min_reader_version: 1,
                },
                metadata: Metadata {
                    id: "f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2".to_owned(),
                    name: None,
                    description: None,
                    format: FileFormat {
                        provider: "parquet".to_owned(),
                    },
                    schema_string: "{\"type\":\"struct\",\"fields\":[{\"name\":\"eventTime\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}".to_owned(),
                    partition_columns: vec!["date".to_owned()],
                    configuration: HashMap::new(),
                    version: None,
                    size: None,
                    num_files: None,
                },
            }));

        let mut state = SharingServerState::new(Arc::new(mock_table_manager));
        state.add_table_reader("DELTA", Arc::new(mock_delta_reader));

        let response = state
            .get_table_metadata("vaccine_share", "acme_vaccine_data", "vaccine_patients")
            .await
            .unwrap();
        assert_json_snapshot!(response);
    }

    #[tokio::test]
    async fn get_table_data() {
        let mut mock_table_manager = MockShareReader::new();
        mock_table_manager
            .expect_get_table()
            .with(
                eq("vaccine_share"),
                eq("acme_vaccine_data"),
                eq("vaccine_patients"),
            )
            .once()
            .returning(|_, _, _| {
                let share = Share::new(
                    "vaccine_share".to_owned(),
                    Some("edacc4a7-6600-4fbb-85f3-a62a5ce6761f".to_owned()),
                );
                let schema = Schema::new(share, "acme_vaccine_data".to_owned(), None);
                Ok(Table::new(
                    schema,
                    "vaccine_patients".to_owned(),
                    Some("c48f3e19-2c29-4ea3-b6f7-3899e53338fa".to_owned()),
                    "s3://vaccine_share/acme_vaccine_data/vaccine_patients".to_owned(),
                    Some("DELTA".to_owned()),
                ))
            });

        let mut mock_delta_reader = MockTableReader::new();
        mock_delta_reader
            .expect_get_table_data()
            .with(eq("s3://vaccine_share/acme_vaccine_data/vaccine_patients"), eq(0u64), eq(None), eq(None))
            .once()
            .return_const(Ok(UnsignedTableData {
                version: 123u64,
                protocol: Protocol {
                    min_reader_version: 1,
                },
                metadata: Metadata {
                    id: "f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2".to_owned(),
                    name: None,
                    description: None,
                    format: FileFormat {
                        provider: "parquet".to_owned(),
                    },
                    schema_string: "{\"type\":\"struct\",\"fields\":[{\"name\":\"eventTime\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}".to_owned(),
                    partition_columns: vec!["date".to_owned()],
                    configuration: HashMap::new(),
                    version: None,
                    size: None,
                    num_files: None,
                },
                data: vec![UnsignedDataFile::File( File {
                    url:"https://test-bucket.s3.eu-west-1.amazonaws.com/file1".to_owned(),
                    id: "8b0086f2-7b27-4935-ac5a-8ed6215a6640".to_owned(),
                    partition_values: HashMap::from([("date".to_owned(), Some("2021-04-28".to_owned()))]),
                    size: 573,
                    stats: Some("{\"numRecords\":1,\"minValues\":{\"eventTime\":\"2021-04-28T23:33:57.955Z\"},\"maxValues\":{\"eventTime\":\"2021-04-28T23:33:57.955Z\"},\"nullCount\":{\"eventTime\":0}}".to_owned()),
                    version: None,
                    timestamp: None,
                    expiration_timestamp: None
                }), UnsignedDataFile::File (File {
                    url:"https://test-bucket.s3.eu-west-1.amazonaws.com/file2".to_owned(),
                    id: "591723a8-6a27-4240-a90e-57426f4736d2".to_owned(),
                    partition_values: HashMap::from([("date".to_owned(), Some("2021-04-28".to_owned()))]),
                    size: 573,
                    stats: Some("{\"numRecords\":1,\"minValues\":{\"eventTime\":\"2021-04-28T23:33:48.719Z\"},\"maxValues\":{\"eventTime\":\"2021-04-28T23:33:48.719Z\"},\"nullCount\":{\"eventTime\":0}}".to_owned()),
                    version: None,
                    timestamp: None ,
                    expiration_timestamp: None
                })],
            }));

        let mut mock_url_signer = MockUrlSigner::new();
        mock_url_signer
            .expect_sign_table_data()
            .times(1)
            .return_const(SignedTableData {
                version: 123u64,
                protocol: Protocol {
                    min_reader_version: 1,
                },
                metadata: Metadata {
                    id: "f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2".to_owned(),
                    name: None,
                    description: None,
                    format: FileFormat {
                        provider: "parquet".to_owned(),
                    },
                    schema_string: "{\"type\":\"struct\",\"fields\":[{\"name\":\"eventTime\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}".to_owned(),
                    partition_columns: vec!["date".to_owned()],
                    configuration: HashMap::new(),
                    version: None,
                    size: None,
                    num_files: None,
                },
                data: vec![SignedDataFile::File( File {
                    url:"https://test-bucket.s3.eu-west-1.amazonaws.com/file1?signature=123".to_owned(),
                    id: "8b0086f2-7b27-4935-ac5a-8ed6215a6640".to_owned(),
                    partition_values: HashMap::from([("date".to_owned(), Some("2021-04-28".to_owned()))]),
                    size: 573,
                    stats: Some("{\"numRecords\":1,\"minValues\":{\"eventTime\":\"2021-04-28T23:33:57.955Z\"},\"maxValues\":{\"eventTime\":\"2021-04-28T23:33:57.955Z\"},\"nullCount\":{\"eventTime\":0}}".to_owned()),
                    version: None,
                    timestamp: None,
                    expiration_timestamp: None
                }), SignedDataFile::File (File {
                    url:"https://test-bucket.s3.eu-west-1.amazonaws.com/file2?signature=123".to_owned(),
                    id: "591723a8-6a27-4240-a90e-57426f4736d2".to_owned(),
                    partition_values: HashMap::from([("date".to_owned(), Some("2021-04-28".to_owned()))]),
                    size: 573,
                    stats: Some("{\"numRecords\":1,\"minValues\":{\"eventTime\":\"2021-04-28T23:33:48.719Z\"},\"maxValues\":{\"eventTime\":\"2021-04-28T23:33:48.719Z\"},\"nullCount\":{\"eventTime\":0}}".to_owned()),
                    version: None,
                    timestamp: None ,
                    expiration_timestamp: None
                })],
            });

        let mut state = SharingServerState::new(Arc::new(mock_table_manager));
        state.add_table_reader("DELTA", Arc::new(mock_delta_reader));
        state.add_url_signer("S3", Arc::new(mock_url_signer));

        let response = state
            .get_table_data(
                "vaccine_share",
                "acme_vaccine_data",
                "vaccine_patients",
                Version::Latest,
            )
            .await
            .unwrap();
        assert_json_snapshot!(response);
    }
}
