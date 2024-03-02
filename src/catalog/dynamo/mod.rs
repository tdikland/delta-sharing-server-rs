//! DynamoDB based implementation of the Catalog trait

use self::{condition::ConditionExt, pagination::PaginationExt};

use super::{Catalog, CatalogError, Page, Pagination, SchemaInfo, ShareInfo, TableInfo};
use crate::auth::ClientId;
use async_trait::async_trait;
use aws_sdk_dynamodb::{
    types::{Delete, Put, TransactWriteItem},
    Client,
};

mod condition;
mod config;
mod convert;
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
    pub async fn _put_share(
        &self,
        client_id: ClientId,
        share: ShareInfo,
    ) -> Result<(), CatalogError> {
        let share_item = convert::to_share_item(client_id, share, &self.config);
        self.client
            .put_item()
            .table_name(self.config.table_name())
            .set_item(Some(share_item))
            .send()
            .await
            .map_err(|e| {
                CatalogError::internal(format!("write share to catalog failed; reason: `{:?}`", e))
            })?;

        Ok(())
    }

    /// Read a share from the catalog
    pub async fn _get_share(
        &self,
        client_id: &ClientId,
        share_name: &str,
    ) -> Result<ShareInfo, CatalogError> {
        let key = convert::to_share_key(client_id, share_name, &self.config);
        let res = self
            .client
            .get_item()
            .table_name(self.config.table_name())
            .set_key(Some(key))
            .send()
            .await
            .map_err(|e| {
                CatalogError::internal(format!("read share from catalog failed; reason: `{:?}`", e))
            })?;

        if let Some(item) = res.item() {
            let share_info = convert::to_share_info(item, &self.config)?;
            Ok(share_info)
        } else {
            Err(CatalogError::share_not_found(share_name))
        }
    }

    /// List all shares in the catalog
    pub async fn _query_shares(
        &self,
        client_id: &ClientId,
        pagination: &Pagination,
    ) -> Result<Page<ShareInfo>, CatalogError> {
        let res = self
            .client
            .query()
            .table_name(self.config.table_name())
            .shares_for_client_cond(client_id, &self.config)
            .with_pagination(pagination)
            .send()
            .await
            .map_err(|e| {
                CatalogError::internal(format!(
                    "list shares from catalog failed; reason: `{:?}`",
                    e
                ))
            })?;

        convert::to_share_info_page(res.items(), res.last_evaluated_key(), &self.config)
    }

    /// Delete a share from the catalog
    pub async fn _delete_share(
        &self,
        client_id: &ClientId,
        share_name: &str,
    ) -> Result<(), CatalogError> {
        let key = convert::to_share_key(client_id, share_name, &self.config);
        self.client
            .transact_write_items()
            .transact_items(
                TransactWriteItem::builder()
                    .condition_check(condition::empty_share_check(
                        client_id,
                        share_name,
                        &self.config,
                    ))
                    .build(),
            )
            .transact_items(
                TransactWriteItem::builder()
                    .delete(
                        Delete::builder()
                            .table_name(self.config.table_name())
                            .set_key(Some(key))
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .send()
            .await
            .map_err(|e| {
                CatalogError::internal(format!(
                    "delete share from catalog failed; reason: `{:?}`",
                    e
                ))
            })?;

        Ok(())
    }

    /// Write a new schema to the catalog
    pub async fn _put_schema(
        &self,
        client_id: ClientId,
        schema: SchemaInfo,
    ) -> Result<(), CatalogError> {
        let item = convert::to_schema_item(client_id.clone(), schema.clone(), &self.config);
        self.client
            .transact_write_items()
            .transact_items(
                TransactWriteItem::builder()
                    .condition_check(condition::share_exists_check(
                        &client_id,
                        schema.share_name(),
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
            .await
            .map_err(|e| {
                // TODO: check if the error is a conditional check failed
                CatalogError::internal(format!("write schema to catalog failed; reason: `{:?}`", e))
            })?;

        Ok(())
    }

    /// Read a schema from the catalog
    pub async fn _get_schema(
        &self,
        client_id: &ClientId,
        share_name: &str,
        schema_name: &str,
    ) -> Result<SchemaInfo, CatalogError> {
        let key = convert::to_schema_key(client_id, share_name, schema_name, &self.config);
        let res = self
            .client
            .get_item()
            .table_name(self.config.table_name())
            .set_key(Some(key))
            .send()
            .await
            .map_err(|e| {
                CatalogError::internal(format!(
                    "read schema from catalog failed; reason: `{:?}`",
                    e
                ))
            })?;

        if let Some(item) = res.item() {
            let schema_info = convert::to_schema_info(item, &self.config)?;
            Ok(schema_info)
        } else {
            Err(CatalogError::schema_not_found(share_name, schema_name))
        }
    }

    /// List all schemas in a share
    pub async fn _query_schemas(
        &self,
        client_id: &ClientId,
        share_name: &str,
        pagination: &Pagination,
    ) -> Result<Page<SchemaInfo>, CatalogError> {
        let res = self
            .client
            .query()
            .table_name(self.config.table_name())
            .schemas_for_client_share_cond(client_id, share_name, &self.config)
            .with_pagination(pagination)
            .send()
            .await
            .map_err(|e| {
                CatalogError::internal(format!(
                    "list schemas from catalog failed; reason: `{:?}`",
                    e
                ))
            })?;

        convert::to_schema_info_page(res.items(), res.last_evaluated_key(), &self.config)
    }

    pub async fn _delete_schema(
        &self,
        client_id: &ClientId,
        share_name: &str,
        schema_name: &str,
    ) -> Result<(), CatalogError> {
        let key = convert::to_schema_key(client_id, share_name, schema_name, &self.config);
        let r = self
            .client
            .transact_write_items()
            .transact_items(
                TransactWriteItem::builder()
                    .condition_check(condition::empty_schema_check(
                        client_id,
                        share_name,
                        schema_name,
                        &self.config,
                    ))
                    .build(),
            )
            .transact_items(
                TransactWriteItem::builder()
                    .delete(
                        Delete::builder()
                            .table_name(self.config.table_name())
                            .set_key(Some(key))
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .send()
            .await
            .map_err(|e| {
                CatalogError::internal(format!(
                    "delete share from catalog failed; reason: `{:?}`",
                    e
                ))
            })?;

        println!("{:?}", r);

        Ok(())
    }

    /// Write a new table to the catalog
    pub async fn _put_table(
        &self,
        client_id: ClientId,
        table: TableInfo,
    ) -> Result<(), CatalogError> {
        let item = convert::to_table_item(client_id.clone(), table.clone(), &self.config);
        self.client
            .transact_write_items()
            .transact_items(
                TransactWriteItem::builder()
                    .condition_check(condition::schema_exists_check(
                        &client_id,
                        table.share_name(),
                        table.schema_name(),
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
            .await
            .map_err(|e| {
                // TODO: check if the error is a conditional check failed
                CatalogError::internal(format!("write table to catalog failed; reason: `{:?}`", e))
            })?;

        Ok(())
    }

    /// Read a table from the catalog
    pub async fn _get_table(
        &self,
        client_id: &ClientId,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<TableInfo, CatalogError> {
        let key =
            convert::to_table_key(client_id, share_name, schema_name, table_name, &self.config);
        let res = self
            .client
            .get_item()
            .table_name(self.config.table_name())
            .set_key(Some(key))
            .send()
            .await
            .map_err(|e| {
                CatalogError::internal(format!("read table to catalog failed; reason: `{:?}`", e))
            })?;

        if let Some(item) = res.item() {
            let table_info = convert::to_table_info(item, &self.config)?;
            Ok(table_info)
        } else {
            Err(CatalogError::table_not_found(
                share_name,
                schema_name,
                table_name,
            ))
        }
    }

    /// List all tables in a share
    pub async fn _query_tables_in_share(
        &self,
        client_id: &ClientId,
        share_name: &str,
        pagination: &Pagination,
    ) -> Result<Page<TableInfo>, CatalogError> {
        let res = self
            .client
            .query()
            .table_name(self.config.table_name())
            .tables_for_client_share_cond(client_id, share_name, &self.config)
            .with_pagination(pagination)
            .send()
            .await
            .map_err(|e| {
                CatalogError::internal(format!("list tables in catalog failed; reason: `{:?}`", e))
            })?;

        convert::to_table_info_page(res.items(), res.last_evaluated_key(), &self.config)
    }

    /// List all tables in a schema
    pub async fn _query_tables_in_schema(
        &self,
        client_id: &ClientId,
        share_name: &str,
        schema_name: &str,
        pagination: &Pagination,
    ) -> Result<Page<TableInfo>, CatalogError> {
        let res = self
            .client
            .query()
            .table_name(self.config.table_name())
            .tables_for_client_schema_cond(&client_id, &share_name, &schema_name, &self.config)
            .with_pagination(pagination)
            .send()
            .await
            .map_err(|e| {
                CatalogError::internal(format!("list tables in catalog failed; reason: `{:?}`", e))
            })?;

        convert::to_table_info_page(res.items(), res.last_evaluated_key(), &self.config)
    }

    pub async fn _delete_table(
        &self,
        _client_id: &ClientId,
        _share_name: &str,
        _schema_name: &str,
        _table_name: &str,
    ) -> Result<(), CatalogError> {
        // Can this be done?
        todo!()
    }
}

#[async_trait]
impl Catalog for DynamoCatalog {
    async fn list_shares(
        &self,
        client_id: &ClientId,
        pagination: &Pagination,
    ) -> Result<Page<ShareInfo>, CatalogError> {
        self._query_shares(client_id, pagination).await
    }

    async fn list_schemas(
        &self,
        client_id: &ClientId,
        share_name: &str,
        pagination: &Pagination,
    ) -> Result<Page<SchemaInfo>, CatalogError> {
        self._query_schemas(client_id, share_name, pagination).await
    }

    async fn list_tables_in_share(
        &self,
        client_id: &ClientId,
        share_name: &str,
        pagination: &Pagination,
    ) -> Result<Page<TableInfo>, CatalogError> {
        self._query_tables_in_share(client_id, share_name, pagination)
            .await
    }

    async fn list_tables_in_schema(
        &self,
        client_id: &ClientId,
        share_name: &str,
        schema_name: &str,
        pagination: &Pagination,
    ) -> Result<Page<TableInfo>, CatalogError> {
        self._query_tables_in_schema(client_id, share_name, schema_name, pagination)
            .await
    }

    async fn get_share(
        &self,
        client_id: &ClientId,
        share_name: &str,
    ) -> Result<ShareInfo, CatalogError> {
        self._get_share(client_id, share_name).await
    }

    async fn get_table(
        &self,
        client_id: &ClientId,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<TableInfo, CatalogError> {
        self._get_table(client_id, share_name, schema_name, table_name)
            .await
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use aws_config::BehaviorVersion;
    use aws_sdk_dynamodb::types::{
        AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ProvisionedThroughput,
        ScalarAttributeType,
    };
    use testcontainers::{clients::Cli, Container, Image};
    use testcontainers_modules::dynamodb_local::DynamoDb;

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
