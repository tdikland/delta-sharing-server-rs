//! DynamoDB based implementation of the Catalog trait

use std::collections::HashMap;

use self::{condition::ConditionExt, pagination::PaginationExt};

use super::{Page, Pagination, Schema, Share, ShareReader, ShareReaderError, Table};
use crate::auth::ClientId;
use async_trait::async_trait;
use aws_sdk_dynamodb::{
    error::SdkError,
    operation::{
        delete_item::DeleteItemError,
        get_item::{GetItemError, GetItemOutput},
        put_item::{PutItemError, PutItemOutput},
        query::{QueryError, QueryOutput},
        transact_write_items::TransactWriteItemsError,
    },
    types::{AttributeValue, Delete, Put, TransactWriteItem},
    Client,
};

mod condition;
mod config;
mod model;
mod pagination;

use aws_sdk_s3::config::IntoShared;
pub use config::DynamoCatalogConfig;

/// Catalog implementation backed by AWS DynamoDB
///
/// AWS DynamoDB is a fully managed NoSQL database service that provides fast
/// and predictable performance with seamless scalability. It uses a schemaless
/// design, which means that items in a table can have different attributes. In
/// this implementation, we use a single table to store all the information
/// related to shares, schemas, and tables. The table has a primary key (PK) and
/// a sort key (SK). The PK is the client ID and the SK is a composite key that
/// includes the type of the item (SHARE, SCHEMA, TABLE) and the name of the
/// item.
///
/// The DynamoDB table has the following schema:
/// For every combination of client_id and securable (i.e. share, schema and table)
/// there is a single item in the table. The item has the following attributes:
/// - PK: The client ID
/// - SK: The sort key (formatted as `<SECURABLE>#<fully_qualified_name>`)
pub struct DynamoCatalog {
    client: Client,
    config: DynamoCatalogConfig,
}

impl DynamoCatalog {
    /// Create a new instance of the DynamoCatalog
    pub fn new(client: Client, config: DynamoCatalogConfig) -> Self {
        Self { client, config }
    }

    /// Write a new share to the catalog
    pub async fn put_share_item(
        &self,
        client_name: &str,
        share_name: &str,
    ) -> Result<(), SdkError<PutItemError>> {
        let share_item = model::build_share_item(client_name, share_name, &self.config);
        self.client
            .put_item()
            .table_name(self.config.table_name())
            .set_item(Some(share_item))
            .send()
            .await?;

        Ok(())
    }

    /// Read a share from the catalog
    pub async fn get_share_item(
        &self,
        client_name: &str,
        share_name: &str,
    ) -> Result<Option<HashMap<String, AttributeValue>>, SdkError<GetItemError>> {
        let key = model::build_share_key(client_name, share_name, &self.config);
        let res = self
            .client
            .get_item()
            .table_name(self.config.table_name())
            .set_key(Some(key))
            .send()
            .await?;

        Ok(res.item)
    }

    /// List all shares in the catalog
    pub async fn query_share_items(
        &self,
        client_name: &str,
        pagination: &Pagination,
    ) -> Result<Vec<HashMap<String, AttributeValue>>, SdkError<QueryError>> {
        let res = self
            .client
            .query()
            .table_name(self.config.table_name())
            .shares_for_client_cond(client_name, &self.config)
            .with_pagination(pagination)
            .send()
            .await?;

        Ok(res.items.unwrap_or_default())
    }

    // /// Delete a share from the catalog
    // pub async fn _delete_share(
    //     &self,
    //     client_id: &ClientId,
    //     share_name: &str,
    // ) -> Result<(), SdkError<TransactWriteItemsError>> {
    //     let key = convert::to_share_key(client_id, share_name, &self.config);
    //     self.client
    //         .transact_write_items()
    //         .transact_items(
    //             TransactWriteItem::builder()
    //                 .condition_check(condition::empty_share_check(
    //                     client_id,
    //                     share_name,
    //                     &self.config,
    //                 ))
    //                 .build(),
    //         )
    //         .transact_items(
    //             TransactWriteItem::builder()
    //                 .delete(
    //                     Delete::builder()
    //                         .table_name(self.config.table_name())
    //                         .set_key(Some(key))
    //                         .build()
    //                         .unwrap(),
    //                 )
    //                 .build(),
    //         )
    //         .send()
    //         .await?;

    //     Ok(())
    // }

    // /// Write a new schema to the catalog
    // pub async fn _put_schema(
    //     &self,
    //     client_id: ClientId,
    //     schema: Schema,
    // ) -> Result<(), SdkError<TransactWriteItemsError>> {
    //     let item = convert::to_schema_item(client_id.clone(), schema.clone(), &self.config);
    //     self.client
    //         .transact_write_items()
    //         .transact_items(
    //             TransactWriteItem::builder()
    //                 .condition_check(condition::share_exists_check(
    //                     &client_id,
    //                     schema.share_name(),
    //                     &self.config,
    //                 ))
    //                 .build(),
    //         )
    //         .transact_items(
    //             TransactWriteItem::builder()
    //                 .put(
    //                     Put::builder()
    //                         .table_name(self.config.table_name())
    //                         .set_item(Some(item))
    //                         .build()
    //                         .unwrap(),
    //                 )
    //                 .build(),
    //         )
    //         .send()
    //         .await?;

    //     Ok(())
    // }

    // /// Read a schema from the catalog
    // pub async fn _get_schema(
    //     &self,
    //     client_id: &ClientId,
    //     share_name: &str,
    //     schema_name: &str,
    // ) -> Result<Option<Schema>, SdkError<GetItemError>> {
    //     let key = convert::to_schema_key(client_id, share_name, schema_name, &self.config);
    //     let res = self
    //         .client
    //         .get_item()
    //         .table_name(self.config.table_name())
    //         .set_key(Some(key))
    //         .send()
    //         .await?;

    //     if let Some(item) = res.item() {
    //         let schema_info = convert::to_schema_info(item, &self.config)?;
    //         Ok(schema_info)
    //     } else {
    //         Ok(None)
    //     }
    // }

    // /// List all schemas in a share
    // pub async fn _query_schemas(
    //     &self,
    //     client_id: &ClientId,
    //     share_name: &str,
    //     pagination: &Pagination,
    // ) -> Result<Page<Schema>, SdkError<QueryError>> {
    //     let res = self
    //         .client
    //         .query()
    //         .table_name(self.config.table_name())
    //         .schemas_for_client_share_cond(client_id, share_name, &self.config)
    //         .with_pagination(pagination)
    //         .send()
    //         .await?;

    //     convert::to_schema_info_page(res.items(), res.last_evaluated_key(), &self.config)
    // }

    // /// Delete a schema from the catalog
    // pub async fn _delete_schema(
    //     &self,
    //     client_id: &ClientId,
    //     share_name: &str,
    //     schema_name: &str,
    // ) -> Result<(), SdkError<TransactWriteItemsError>> {
    //     let key = convert::to_schema_key(client_id, share_name, schema_name, &self.config);
    //     let r = self
    //         .client
    //         .transact_write_items()
    //         .transact_items(
    //             TransactWriteItem::builder()
    //                 .condition_check(condition::empty_schema_check(
    //                     client_id,
    //                     share_name,
    //                     schema_name,
    //                     &self.config,
    //                 ))
    //                 .build(),
    //         )
    //         .transact_items(
    //             TransactWriteItem::builder()
    //                 .delete(
    //                     Delete::builder()
    //                         .table_name(self.config.table_name())
    //                         .set_key(Some(key))
    //                         .build()
    //                         .unwrap(),
    //                 )
    //                 .build(),
    //         )
    //         .send()
    //         .await?;

    //     println!("{:?}", r);

    //     Ok(())
    // }

    // /// Write a new table to the catalog
    // pub async fn _put_table(
    //     &self,
    //     client_id: ClientId,
    //     table: Table,
    // ) -> Result<(), SdkError<TransactWriteItemsError>> {
    //     let item = convert::to_table_item(client_id.clone(), table.clone(), &self.config);
    //     self.client
    //         .transact_write_items()
    //         .transact_items(
    //             TransactWriteItem::builder()
    //                 .condition_check(condition::schema_exists_check(
    //                     &client_id,
    //                     table.share_name(),
    //                     table.schema_name(),
    //                     &self.config,
    //                 ))
    //                 .build(),
    //         )
    //         .transact_items(
    //             TransactWriteItem::builder()
    //                 .put(
    //                     Put::builder()
    //                         .table_name(self.config.table_name())
    //                         .set_item(Some(item))
    //                         .build()
    //                         .unwrap(),
    //                 )
    //                 .build(),
    //         )
    //         .send()
    //         .await?;

    //     Ok(())
    // }

    // /// Read a table from the catalog
    // pub async fn _get_table(
    //     &self,
    //     client_id: &ClientId,
    //     share_name: &str,
    //     schema_name: &str,
    //     table_name: &str,
    // ) -> Result<Option<Table>, SdkError<GetItemError>> {
    //     let key =
    //         convert::to_table_key(client_id, share_name, schema_name, table_name, &self.config);
    //     let res = self
    //         .client
    //         .get_item()
    //         .table_name(self.config.table_name())
    //         .set_key(Some(key))
    //         .send()
    //         .await?;

    //     if let Some(item) = res.item() {
    //         let table_info = convert::to_table_info(item, &self.config)?;
    //         Ok(table_info)
    //     } else {
    //         Ok(None)
    //     }
    // }

    // /// List all tables in a share
    // pub async fn _query_tables_in_share(
    //     &self,
    //     client_id: &ClientId,
    //     share_name: &str,
    //     pagination: &Pagination,
    // ) -> Result<Page<Table>, SdkError<QueryError>> {
    //     let res = self
    //         .client
    //         .query()
    //         .table_name(self.config.table_name())
    //         .tables_for_client_share_cond(client_id, share_name, &self.config)
    //         .with_pagination(pagination)
    //         .send()
    //         .await?;

    //     convert::to_table_info_page(res.items(), res.last_evaluated_key(), &self.config)
    // }

    // /// List all tables in a schema
    // pub async fn _query_tables_in_schema(
    //     &self,
    //     client_id: &ClientId,
    //     share_name: &str,
    //     schema_name: &str,
    //     pagination: &Pagination,
    // ) -> Result<Page<Table>, SdkError<QueryError>> {
    //     let res = self
    //         .client
    //         .query()
    //         .table_name(self.config.table_name())
    //         .tables_for_client_schema_cond(client_id, share_name, schema_name, &self.config)
    //         .with_pagination(pagination)
    //         .send()
    //         .await?;

    //     convert::to_table_info_page(res.items(), res.last_evaluated_key(), &self.config)
    // }

    // /// Delete a table from the catalog
    // pub async fn _delete_table(
    //     &self,
    //     _client_id: &ClientId,
    //     _share_name: &str,
    //     _schema_name: &str,
    //     _table_name: &str,
    // ) -> Result<(), SdkError<DeleteItemError>> {
    //     // Can this be done?
    //     todo!()
    // }
}

#[async_trait]
impl ShareReader for DynamoCatalog {
    async fn list_shares(
        &self,
        client_id: &ClientId,
        pagination: &Pagination,
    ) -> Result<Page<Share>, ShareReaderError> {
        // self._query_shares(client_id, pagination)
        //     .await
        //     .map_err(Into::into)
        todo!()
    }

    async fn list_schemas(
        &self,
        client_id: &ClientId,
        share_name: &str,
        pagination: &Pagination,
    ) -> Result<Page<Schema>, ShareReaderError> {
        // self._query_schemas(client_id, share_name, pagination)
        //     .await
        //     .map_err(Into::into)
        todo!()
    }

    async fn list_tables_in_share(
        &self,
        client_id: &ClientId,
        share_name: &str,
        pagination: &Pagination,
    ) -> Result<Page<Table>, ShareReaderError> {
        // self._query_tables_in_share(client_id, share_name, pagination)
        //     .await
        //     .map_err(Into::into)
        todo!()
    }

    async fn list_tables_in_schema(
        &self,
        client_id: &ClientId,
        share_name: &str,
        schema_name: &str,
        pagination: &Pagination,
    ) -> Result<Page<Table>, ShareReaderError> {
        // self._query_tables_in_schema(client_id, share_name, schema_name, pagination)
        //     .await
        //     .map_err(Into::into)
        todo!()
    }

    async fn get_share(
        &self,
        client_id: &ClientId,
        share_name: &str,
    ) -> Result<Share, ShareReaderError> {
        self._get_share(client_id, share_name)
            .await
            .map_err(Into::into)
            .and_then(|res| res.ok_or_else(|| ShareReaderError::not_found("share not found")))
    }

    async fn get_table(
        &self,
        client_id: &ClientId,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Table, ShareReaderError> {
        // self._get_table(client_id, share_name, schema_name, table_name)
        //     .await
        //     .map_err(Into::into)
        //     .and_then(|res| res.ok_or_else(|| ShareReaderError::not_found("table not found")))
        todo!()
    }
}

impl<E> From<SdkError<E>> for ShareReaderError {
    fn from(value: SdkError<E>) -> Self {
        ShareReaderError::internal(value.to_string())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use aws_config::Region;
    use aws_sdk_dynamodb::{config::Credentials, Config};
    use aws_sdk_s3::primitives::SdkBody;
    use aws_smithy_runtime::client::http::test_util::{ReplayEvent, StaticReplayClient};

    #[tokio::test]
    async fn test_put_share() {
        let http_client = StaticReplayClient::new(vec![ReplayEvent::new(
            http::Request::builder()
                .uri("https://dynamodb.eu-west-1.amazonaws.com/")
                .body(SdkBody::from(r#"{"TableName":"test-table","Key":{"SK":{"S":"SHARE#share1"},"PK":{"S":"test-client"}}}"#))
                .unwrap(),
            http::Response::builder()
                .status(404)
                .body(SdkBody::from(
                    r#"{"Items":[{"PK":{"S":"test-client"},"SK":{"S":"SHARE#share1"}},{"PK":{"S":"test-client"},"SK":{"S":"SHARE#share2"}}],"Count":2,"ScannedCount":2}"#,
                ))
                .unwrap(),
        )]);

        let conf = Config::builder()
            .http_client(http_client.clone())
            .region(Region::new("eu-west-1"))
            .credentials_provider(Credentials::for_tests())
            .behavior_version_latest()
            .build();
        let client = Client::from_conf(conf);

        let catalog_config = DynamoCatalogConfig::new("test-table");
        let catalog = DynamoCatalog::new(client, catalog_config);
        let client_id = ClientId::known("test-client");

        let res = catalog._put_share("foo", "bar").await;

        http_client.assert_requests_match(&[]);
    }

    // use aws_config::BehaviorVersion;
    // use aws_sdk_dynamodb::types::{
    //     AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ProvisionedThroughput,
    //     ScalarAttributeType,
    // };
    // use testcontainers::{clients::Cli, Container, Image};
    // use testcontainers_modules::dynamodb_local::DynamoDb;

    // #[tokio::test]
    // async fn test_parse_share_info() {
    //     let mut item = HashMap::new();
    //     item.insert(
    //         "share_name".to_owned(),
    //         AttributeValue::S("test-share".to_owned()),
    //     );

    //     let share_info: ShareInfo = (&item).try_into().unwrap();
    //     assert_eq!(share_info.name(), "test-share");
    // }

    // #[tokio::test]
    // async fn test_parse_schema_info() {
    //     let mut item = HashMap::new();
    //     item.insert(
    //         "schema_name".to_owned(),
    //         AttributeValue::S("test-schema".to_owned()),
    //     );
    //     item.insert(
    //         "share_name".to_owned(),
    //         AttributeValue::S("test-share".to_owned()),
    //     );

    //     let schema_info: SchemaInfo = (&item).try_into().unwrap();
    //     assert_eq!(schema_info.name(), "test-schema");
    //     assert_eq!(schema_info.share_name(), "test-share");
    // }
}
