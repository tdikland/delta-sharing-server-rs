//! DynamoDB based implementation of the Catalog trait

use std::collections::HashMap;

use self::{condition::ConditionExt, pagination::PaginationExt};

use super::{Page, Pagination, Schema, Share, ShareReader, ShareReaderError, Table};
use crate::{auth::ClientId, catalog::dynamo::pagination::key_to_token};
use async_trait::async_trait;
use aws_sdk_dynamodb::{
    error::SdkError,
    operation::{
        get_item::{GetItemError, GetItemOutput},
        put_item::PutItemError,
        query::{QueryError, QueryOutput},
        transact_write_items::TransactWriteItemsError,
    },
    types::{AttributeValue, Put, TransactWriteItem},
    Client,
};

mod condition;
mod config;
mod model;
mod pagination;

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
    ) -> Result<GetItemOutput, SdkError<GetItemError>> {
        let key = model::build_share_key(client_name, share_name, &self.config);
        self.client
            .get_item()
            .table_name(self.config.table_name())
            .set_key(Some(key))
            .send()
            .await
    }

    /// List all shares in the catalog
    pub async fn query_share_items(
        &self,
        client_name: &str,
        pagination: &Pagination,
    ) -> Result<QueryOutput, SdkError<QueryError>> {
        self.client
            .query()
            .table_name(self.config.table_name())
            .shares_for_client_cond(client_name, &self.config)
            .with_pagination(pagination)
            .send()
            .await
    }

    async fn delete_share_item(&self) {
        todo!()
    }

    /// Write a new schema to the catalog
    pub async fn put_schema_item(
        &self,
        client_name: &str,
        share_name: &str,
        schema_name: &str,
    ) -> Result<(), SdkError<TransactWriteItemsError>> {
        let item = model::build_schema_item(client_name, share_name, schema_name, &self.config);
        self.client
            .transact_write_items()
            .transact_items(
                TransactWriteItem::builder()
                    .condition_check(condition::share_exists_check(
                        client_name,
                        share_name,
                        &self.config,
                    ))
                    .build(),
            )
            .transact_items(
                TransactWriteItem::builder()
                    .put(
                        Put::builder()
                            .table_name(self.config.table_name())
                            .set_item(Some(item))
                            .build()
                            .expect("valid put item call"),
                    )
                    .build(),
            )
            .send()
            .await?;

        Ok(())
    }

    /// Read a schema from the catalog
    pub async fn get_schema_item(
        &self,
        client_id: &str,
        share_name: &str,
        schema_name: &str,
    ) -> Result<GetItemOutput, SdkError<GetItemError>> {
        let key = model::build_schema_key(client_id, share_name, schema_name, &self.config);
        self.client
            .get_item()
            .table_name(self.config.table_name())
            .set_key(Some(key))
            .send()
            .await
    }

    /// List all schemas in a share
    pub async fn query_schema_items(
        &self,
        client_name: &str,
        share_name: &str,
        pagination: &Pagination,
    ) -> Result<QueryOutput, SdkError<QueryError>> {
        self.client
            .query()
            .table_name(self.config.table_name())
            .schemas_for_client_share_cond(client_name, share_name, &self.config)
            .with_pagination(pagination)
            .send()
            .await
    }

    async fn delete_schema_item(&self) {
        todo!()
    }

    /// Write a new table to the catalog
    pub async fn put_table_item(
        &self,
        client_name: &str,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
        storage_path: &str,
    ) -> Result<(), SdkError<TransactWriteItemsError>> {
        let item = model::build_table_item(
            client_name,
            share_name,
            schema_name,
            table_name,
            storage_path,
            &self.config,
        );
        self.client
            .transact_write_items()
            .transact_items(
                TransactWriteItem::builder()
                    .condition_check(condition::schema_exists_check(
                        client_name,
                        share_name,
                        table_name,
                        &self.config,
                    ))
                    .build(),
            )
            .transact_items(
                TransactWriteItem::builder()
                    .put(
                        Put::builder()
                            .table_name(self.config.table_name())
                            .set_item(Some(item))
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .send()
            .await?;

        Ok(())
    }

    /// Read a table from the catalog
    pub async fn get_table_item(
        &self,
        client_id: &str,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<GetItemOutput, SdkError<GetItemError>> {
        let key =
            model::build_table_key(client_id, share_name, schema_name, table_name, &self.config);
        self.client
            .get_item()
            .table_name(self.config.table_name())
            .set_key(Some(key))
            .send()
            .await
    }

    /// List all tables in a share
    pub async fn query_table_items_in_share(
        &self,
        client_name: &str,
        share_name: &str,
        pagination: &Pagination,
    ) -> Result<QueryOutput, SdkError<QueryError>> {
        self.client
            .query()
            .table_name(self.config.table_name())
            .tables_for_client_share_cond(client_name, share_name, &self.config)
            .with_pagination(pagination)
            .send()
            .await
    }

    /// List all tables in a schema
    pub async fn query_table_items_in_schema(
        &self,
        client_name: &str,
        share_name: &str,
        schema_name: &str,
        pagination: &Pagination,
    ) -> Result<QueryOutput, SdkError<QueryError>> {
        self.client
            .query()
            .table_name(self.config.table_name())
            .tables_for_client_schema_cond(client_name, share_name, schema_name, &self.config)
            .with_pagination(pagination)
            .send()
            .await
    }

    async fn delete_table_item(&self) {
        todo!()
    }
}

#[async_trait]
impl ShareReader for DynamoCatalog {
    async fn list_shares(
        &self,
        client_id: &ClientId,
        pagination: &Pagination,
    ) -> Result<Page<Share>, ShareReaderError> {
        let output = self.query_share_items(client_id, pagination).await?;
        let shares = output
            .items
            .unwrap_or_default()
            .into_iter()
            .map(|item| model::item_to_share(&item, &self.config))
            .collect::<Result<Vec<Share>, ShareReaderError>>()?;
        let token = output.last_evaluated_key.map(key_to_token);

        Ok(Page::new(shares, token))
    }

    async fn list_schemas(
        &self,
        client_id: &ClientId,
        share_name: &str,
        pagination: &Pagination,
    ) -> Result<Page<Schema>, ShareReaderError> {
        let output = self
            .query_schema_items(client_id, share_name, pagination)
            .await?;
        let schemas = output
            .items
            .unwrap_or_default()
            .into_iter()
            .map(|item| model::item_to_schema(&item, &self.config))
            .collect::<Result<Vec<Schema>, ShareReaderError>>()?;
        let token = output.last_evaluated_key.map(key_to_token);

        Ok(Page::new(schemas, token))
    }

    async fn list_tables_in_share(
        &self,
        client_id: &ClientId,
        share_name: &str,
        pagination: &Pagination,
    ) -> Result<Page<Table>, ShareReaderError> {
        let output = self
            .query_table_items_in_share(client_id, share_name, pagination)
            .await?;
        let schemas = output
            .items
            .unwrap_or_default()
            .into_iter()
            .map(|item| model::item_to_table(&item, &self.config))
            .collect::<Result<Vec<Table>, ShareReaderError>>()?;
        let token = output.last_evaluated_key.map(key_to_token);

        Ok(Page::new(schemas, token))
    }

    async fn list_tables_in_schema(
        &self,
        client_id: &ClientId,
        share_name: &str,
        schema_name: &str,
        pagination: &Pagination,
    ) -> Result<Page<Table>, ShareReaderError> {
        let output = self
            .query_table_items_in_schema(client_id, share_name, schema_name, pagination)
            .await?;
        let schemas = output
            .items
            .unwrap_or_default()
            .into_iter()
            .map(|item| model::item_to_table(&item, &self.config))
            .collect::<Result<Vec<Table>, ShareReaderError>>()?;
        let token = output.last_evaluated_key.map(key_to_token);

        Ok(Page::new(schemas, token))
    }

    async fn get_share(
        &self,
        client_id: &ClientId,
        share_name: &str,
    ) -> Result<Share, ShareReaderError> {
        let output = self.get_share_item(client_id, share_name).await?;
        let item = output.item.ok_or(ShareReaderError::not_found(format!(
            "share `{share_name}` was not found."
        )))?;

        model::item_to_share(&item, &self.config)
    }

    async fn get_table(
        &self,
        client_id: &ClientId,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Table, ShareReaderError> {
        let output = self
            .get_table_item(client_id, share_name, schema_name, table_name)
            .await?;
        let item = output.item.ok_or(ShareReaderError::not_found(format!(
            "table `{share_name}.{schema_name}.{table_name}` was not found."
        )))?;

        model::item_to_table(&item, &self.config)
    }
}

impl<E> From<SdkError<E>> for ShareReaderError {
    fn from(value: SdkError<E>) -> Self {
        ShareReaderError::internal(value.to_string())
    }
}
