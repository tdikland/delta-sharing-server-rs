//! DynamoDB based implementation of the Catalog trait

use super::{Catalog, CatalogError, Page, Pagination, SchemaInfo, ShareInfo, TableInfo};
use crate::auth::ClientId;
use async_trait::async_trait;
use aws_sdk_dynamodb::{types::AttributeValue, Client};
use base64::{engine::general_purpose, Engine as _};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

const DYNAMO_ATTRIBUTE_PK: &'static str = "PK";
const DYNAMO_ATTRIBUTE_SK: &'static str = "SK";
const DYNAMO_ATTRIBUTE_SHARE_ID: &'static str = "share_id";
const DYNAMO_ATTRIBUTE_SHARE_NAME: &'static str = "share_name";
const DYNAMO_ATTRIBUTE_SCHEMA_NAME: &'static str = "schema_name";
const DYNAMO_ATTRIBUTE_TABLE_ID: &'static str = "table_id";
const DYNAMO_ATTRIBUTE_TABLE_NAME: &'static str = "table_name";
const DYNAMO_ATTRIBUTE_TABLE_STORAGE_LOCATION: &'static str = "table_storage_location";

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
    table_name: String,
}

impl DynamoCatalog {
    /// Create a new instance of the DynamoCatalog
    pub fn new(client: Client, table_name: impl Into<String>) -> Self {
        Self {
            client,
            table_name: table_name.into(),
        }
    }

    /// Write a new share to the catalog
    pub async fn put_share(&self, client_id: String, share: ShareInfo) -> Result<(), CatalogError> {
        let mut builder = self
            .client
            .put_item()
            .table_name(&self.table_name)
            .item(DYNAMO_ATTRIBUTE_PK, AttributeValue::S(client_id))
            .item(
                DYNAMO_ATTRIBUTE_SK,
                AttributeValue::S(format!("SHARE#{}", share.name())),
            )
            .item(
                DYNAMO_ATTRIBUTE_SHARE_NAME,
                AttributeValue::S(share.name().to_owned()),
            );

        if let Some(share_id) = share.id() {
            builder = builder.item(
                DYNAMO_ATTRIBUTE_SHARE_ID,
                AttributeValue::S(share_id.to_owned()),
            );
        }

        let _result = builder.send().await.map_err(|e| {
            CatalogError::internal(format!(
                "write share to catalog failed; reason: `{}`",
                e.to_string()
            ))
        })?;

        Ok(())
    }

    pub async fn put_shares(
        &self,
        client_id: String,
        shares: &[ShareInfo],
    ) -> Result<(), CatalogError> {
        for share in shares {
            self.put_share(client_id.clone(), share.clone()).await?;
        }

        Ok(())
    }

    /// Read a share from the catalog
    pub async fn _get_share(
        &self,
        client_id: &str,
        share_name: &str,
    ) -> Result<ShareInfo, CatalogError> {
        let result = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key("PK", AttributeValue::S(client_id.to_owned()))
            .key("SK", AttributeValue::S(format!("SHARE#{}", share_name)))
            .send()
            .await
            .map_err(|e| {
                println!("{e:?}");
                CatalogError::internal(e.to_string())
            })?;

        if let Some(item) = result.item() {
            Ok(item.try_into()?)
        } else {
            Err(CatalogError::share_not_found(share_name))
        }
    }

    /// List all shares in the catalog
    pub async fn query_shares(
        &self,
        client_id: String,
        pagination: &Pagination,
    ) -> Result<Page<ShareInfo>, CatalogError> {
        let mut query = self
            .client
            .query()
            .table_name(&self.table_name)
            .expression_attribute_names("#PK", "PK")
            .expression_attribute_names("#SK", "SK")
            .expression_attribute_values(":pk", AttributeValue::S(client_id))
            .expression_attribute_values(":sk", AttributeValue::S("SHARE".to_owned()))
            .key_condition_expression("#PK = :pk AND begins_with(#SK, :sk)");

        if let Some(max_results) = pagination.max_results() {
            query = query.limit(max_results as i32);
        }

        if let Some(token) = pagination.page_token() {
            query = query.set_exclusive_start_key(Some(token_to_key(token)));
        }

        let result = query
            .send()
            .await
            .map_err(|e| CatalogError::internal(e.to_string()))?;

        let shares = result
            .items()
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<ShareInfo>, CatalogError>>()?;

        Ok(Page::new(
            shares,
            result.last_evaluated_key().map(key_to_token),
        ))
    }

    /// Write a new schema to the catalog
    pub async fn put_schema(
        &self,
        client_id: String,
        schema: SchemaInfo,
    ) -> Result<(), CatalogError> {
        let builder = self
            .client
            .put_item()
            .table_name(&self.table_name)
            .item("PK", AttributeValue::S(client_id))
            .item(
                "SK",
                AttributeValue::S(format!("SCHEMA#{}.{}", schema.share_name(), schema.name())),
            )
            .item(
                DYNAMO_ATTRIBUTE_SHARE_NAME,
                AttributeValue::S(schema.share_name().to_owned()),
            )
            .item(
                DYNAMO_ATTRIBUTE_SCHEMA_NAME,
                AttributeValue::S(schema.name().to_owned()),
            );

        let _result = builder
            .send()
            .await
            .map_err(|e| CatalogError::internal(e.to_string()))?;

        Ok(())
    }

    /// Read a schema from the catalog
    pub async fn get_schema(
        &self,
        client_id: &str,
        share_name: &str,
        schema_name: &str,
    ) -> Result<SchemaInfo, CatalogError> {
        let result = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key("PK", AttributeValue::S(client_id.to_owned()))
            .key(
                "SK",
                AttributeValue::S(format!("SCHEMA#{}.{}", share_name, schema_name)),
            )
            .send()
            .await
            .map_err(|e| {
                println!("{:?}", e);
                CatalogError::internal(e.to_string())
            })?;

        if let Some(item) = result.item() {
            Ok(item.try_into()?)
        } else {
            Err(CatalogError::schema_not_found(share_name, schema_name))
        }
    }

    /// List all schemas in a share
    pub async fn query_schemas(
        &self,
        client_id: String,
        share_name: String,
        pagination: &Pagination,
    ) -> Result<Page<SchemaInfo>, CatalogError> {
        let mut query = self
            .client
            .query()
            .table_name(&self.table_name)
            .expression_attribute_names("#PK", "PK")
            .expression_attribute_names("#SK", "SK")
            .expression_attribute_values(":pk", AttributeValue::S(client_id))
            .expression_attribute_values(
                ":sk",
                AttributeValue::S(format!("SCHEMA#{}.", share_name)),
            )
            .key_condition_expression("#PK = :pk AND begins_with(#SK, :sk)");

        if let Some(max_results) = pagination.max_results() {
            query = query.limit(max_results as i32);
        }

        if let Some(token) = pagination.page_token() {
            query = query.set_exclusive_start_key(Some(token_to_key(token)));
        }

        let result = query
            .send()
            .await
            .map_err(|e| CatalogError::internal(e.to_string()))?;

        let schemas = result
            .items()
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<SchemaInfo>, CatalogError>>()?;

        Ok(Page::new(
            schemas,
            result.last_evaluated_key().map(key_to_token),
        ))
    }

    /// Write a new table to the catalog
    pub async fn put_table(&self, client_id: String, table: TableInfo) -> Result<(), CatalogError> {
        let builder = self
            .client
            .put_item()
            .table_name(&self.table_name)
            .item("PK", AttributeValue::S(client_id))
            .item(
                "SK",
                AttributeValue::S(format!(
                    "TABLE#{}.{}.{}",
                    table.share_name(),
                    table.schema_name(),
                    table.name()
                )),
            )
            .item("table_name", AttributeValue::S(table.name().to_owned()))
            .item(
                "schema_name",
                AttributeValue::S(table.schema_name().to_owned()),
            )
            .item(
                "share_name",
                AttributeValue::S(table.share_name().to_owned()),
            )
            .item(
                "storage_location",
                AttributeValue::S(table.storage_path().to_owned()),
            );

        let _result = builder
            .send()
            .await
            .map_err(|e| CatalogError::internal(e.to_string()))?;

        Ok(())
    }

    /// Read a table from the catalog
    pub async fn _get_table(
        &self,
        client_id: String,
        share_name: String,
        schema_name: String,
        table_name: String,
    ) -> Result<TableInfo, CatalogError> {
        let result = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key("PK", AttributeValue::S(client_id))
            .key(
                "SK",
                AttributeValue::S(format!(
                    "TABLE#{}.{}.{}",
                    share_name, schema_name, table_name
                )),
            )
            .send()
            .await
            .map_err(|e| CatalogError::internal(e.to_string()))?;

        if let Some(item) = result.item() {
            Ok(item.try_into()?)
        } else {
            Err(CatalogError::table_not_found(
                &share_name,
                &schema_name,
                &table_name,
            ))
        }
    }

    /// List all tables in a share
    pub async fn query_tables_in_share(
        &self,
        client_id: String,
        share_name: String,
        pagination: &Pagination,
    ) -> Result<Page<TableInfo>, CatalogError> {
        let mut query = self
            .client
            .query()
            .table_name(&self.table_name)
            .expression_attribute_names("#PK", "PK")
            .expression_attribute_names("#SK", "SK")
            .expression_attribute_values(":pk", AttributeValue::S(client_id))
            .expression_attribute_values(":sk", AttributeValue::S(format!("TABLE#{}.", share_name)))
            .key_condition_expression("#PK = :pk AND begins_with(#SK, :sk)");

        if let Some(max_results) = pagination.max_results() {
            query = query.limit(max_results as i32);
        }

        if let Some(token) = pagination.page_token() {
            query = query.set_exclusive_start_key(Some(token_to_key(token)));
        }

        let result = query
            .send()
            .await
            .map_err(|e| CatalogError::internal(e.to_string()))?;

        let tables = result
            .items()
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<TableInfo>, CatalogError>>()?;

        Ok(Page::new(
            tables,
            result.last_evaluated_key().map(key_to_token),
        ))
    }

    /// List all tables in a schema
    pub async fn query_tables_in_schema(
        &self,
        client_id: String,
        share_name: String,
        schema_name: String,
        pagination: &Pagination,
    ) -> Result<Page<TableInfo>, CatalogError> {
        let mut query = self
            .client
            .query()
            .table_name(&self.table_name)
            .expression_attribute_names("#PK", "PK")
            .expression_attribute_names("#SK", "SK")
            .expression_attribute_values(":pk", AttributeValue::S(client_id))
            .expression_attribute_values(
                ":sk",
                AttributeValue::S(format!("TABLE#{}.{}.", share_name, schema_name)),
            )
            .key_condition_expression("#PK = :pk AND begins_with(#SK, :sk)");

        if let Some(max_results) = pagination.max_results() {
            query = query.limit(max_results as i32);
        }

        if let Some(token) = pagination.page_token() {
            query = query.set_exclusive_start_key(Some(token_to_key(token)));
        }

        let result = query
            .send()
            .await
            .map_err(|e| CatalogError::internal(e.to_string()))?;

        let tables = result
            .items()
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<TableInfo>, CatalogError>>()?;

        Ok(Page::new(
            tables,
            result.last_evaluated_key().map(key_to_token),
        ))
    }
}

/// A struct to represent the primary key of the DynamoDB table
#[derive(Serialize, Deserialize)]
struct DynamoKey {
    pk: String,
    sk: String,
}

/// Convert a pagination token to a DynamoDB key
fn token_to_key(token: &str) -> HashMap<String, AttributeValue> {
    let decoded_token = general_purpose::URL_SAFE.decode(token).unwrap();
    let key: DynamoKey = serde_json::from_slice(&decoded_token).unwrap();
    let map = HashMap::from_iter([
        (String::from("PK"), AttributeValue::S(key.pk)),
        (String::from("SK"), AttributeValue::S(key.sk)),
    ]);
    map
}

/// Convert a DynamoDB key to a pagination token
fn key_to_token(key: &HashMap<String, AttributeValue>) -> String {
    let dynamo_key = DynamoKey {
        pk: key.get("PK").unwrap().as_s().unwrap().to_owned(),
        sk: key.get("SK").unwrap().as_s().unwrap().to_owned(),
    };
    let json = serde_json::to_vec(&dynamo_key).unwrap();
    general_purpose::URL_SAFE.encode(&json)
}

impl TryFrom<&HashMap<String, AttributeValue>> for ShareInfo {
    type Error = CatalogError;

    fn try_from(value: &HashMap<String, AttributeValue>) -> Result<Self, Self::Error> {
        let share_name = extract_from_item(value, "share_name")?;
        let share_id = extract_from_item_opt(value, "share_id");

        Ok(ShareInfo::new(share_name, share_id))
    }
}

impl TryFrom<&HashMap<String, AttributeValue>> for SchemaInfo {
    type Error = CatalogError;

    fn try_from(value: &HashMap<String, AttributeValue>) -> Result<Self, Self::Error> {
        let name = extract_from_item(value, DYNAMO_ATTRIBUTE_SCHEMA_NAME)?;
        let share_name = extract_from_item(value, DYNAMO_ATTRIBUTE_SHARE_NAME)?;

        Ok(Self::new(name, share_name))
    }
}

impl TryFrom<&HashMap<String, AttributeValue>> for TableInfo {
    type Error = CatalogError;

    fn try_from(item: &HashMap<String, AttributeValue>) -> Result<Self, Self::Error> {
        let name = extract_from_item(item, "table_name")?;
        let schema_name = extract_from_item(item, "schema_name")?;
        let share_name = extract_from_item(item, "share_name")?;
        let storage_location = extract_from_item(item, "storage_location")?;
        let _id = extract_from_item_opt(item, "table_id");
        let _share_id = extract_from_item_opt(item, "share_id");

        Ok(Self::new(name, schema_name, share_name, storage_location))
    }
}

fn extract_from_item(
    item: &HashMap<String, AttributeValue>,
    key: &str,
) -> Result<String, CatalogError> {
    item.get(key)
        .ok_or(CatalogError::internal(format!(
            "attribute `{}` not found in item",
            key
        )))?
        .as_s()
        .map_err(|_| CatalogError::internal(format!("attribute `{}` was not a string", key)))
        .cloned()
}

fn extract_from_item_opt(item: &HashMap<String, AttributeValue>, key: &str) -> Option<String> {
    item.get(key).and_then(|v| v.as_s().ok().cloned())
}

#[async_trait]
impl Catalog for DynamoCatalog {
    async fn list_shares(
        &self,
        client_id: &ClientId,
        pagination: &Pagination,
    ) -> Result<Page<ShareInfo>, CatalogError> {
        self.query_shares(client_id.to_string(), pagination).await
    }

    async fn list_schemas(
        &self,
        client_id: &ClientId,
        share_name: &str,
        cursor: &Pagination,
    ) -> Result<Page<SchemaInfo>, CatalogError> {
        self.query_schemas(client_id.to_string(), share_name.to_owned(), cursor)
            .await
    }

    async fn list_tables_in_share(
        &self,
        client_id: &ClientId,
        share_name: &str,
        cursor: &Pagination,
    ) -> Result<Page<TableInfo>, CatalogError> {
        self.query_tables_in_share(client_id.to_string(), share_name.to_owned(), cursor)
            .await
    }

    async fn list_tables_in_schema(
        &self,
        client_id: &ClientId,
        share_name: &str,
        schema_name: &str,
        cursor: &Pagination,
    ) -> Result<Page<TableInfo>, CatalogError> {
        self.query_tables_in_schema(
            client_id.to_string(),
            share_name.to_owned(),
            schema_name.to_owned(),
            cursor,
        )
        .await
    }

    async fn get_share(
        &self,
        client_id: &ClientId,
        share_name: &str,
    ) -> Result<ShareInfo, CatalogError> {
        self._get_share(&client_id.to_string(), share_name).await
    }

    async fn get_table(
        &self,
        client_id: &ClientId,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<TableInfo, CatalogError> {
        self._get_table(
            client_id.to_string(),
            share_name.to_owned(),
            schema_name.to_owned(),
            table_name.to_owned(),
        )
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

    #[tokio::test]
    async fn test_parse_share_info() {
        let mut item = HashMap::new();
        item.insert(
            "share_name".to_owned(),
            AttributeValue::S("test-share".to_owned()),
        );

        let share_info: ShareInfo = (&item).try_into().unwrap();
        assert_eq!(share_info.name(), "test-share");
    }

    #[tokio::test]
    async fn test_parse_schema_info() {
        let mut item = HashMap::new();
        item.insert(
            "schema_name".to_owned(),
            AttributeValue::S("test-schema".to_owned()),
        );
        item.insert(
            "share_name".to_owned(),
            AttributeValue::S("test-share".to_owned()),
        );

        let schema_info: SchemaInfo = (&item).try_into().unwrap();
        assert_eq!(schema_info.name(), "test-schema");
        assert_eq!(schema_info.share_name(), "test-share");
    }

    #[tokio::test]
    async fn schema_curd() {
        let docker = Cli::default();
        let dynamo = DynamoDb::default();
        let container = docker.run(dynamo);

        let client = init_client(&container).await;
        let catalog = init_catalog(client).await;

        let schema1 = catalog
            .get_schema("ANONYMOUS", "share1", "schema1")
            .await
            .unwrap();

        assert_eq!(schema1.share_name(), "share1");
        assert_eq!(schema1.name(), "schema1");
    }

    async fn init_client<I: Image>(container: &Container<'_, I>) -> Client {
        let endpoint_uri = format!("http://127.0.0.1:{}", container.get_host_port_ipv4(8000));
        let shared_config = aws_config::defaults(BehaviorVersion::latest())
            .endpoint_url(endpoint_uri)
            .load()
            .await;
        Client::new(&shared_config)
    }

    async fn init_catalog(client: Client) -> DynamoCatalog {
        let table_name = String::from("test-table");

        // Create DynamoDB table
        client
            .create_table()
            .table_name(&table_name)
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("PK")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("SK")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("PK")
                    .key_type(KeyType::Hash)
                    .build()
                    .unwrap(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("SK")
                    .key_type(KeyType::Range)
                    .build()
                    .unwrap(),
            )
            .billing_mode(BillingMode::Provisioned)
            .provisioned_throughput(
                ProvisionedThroughput::builder()
                    .read_capacity_units(5)
                    .write_capacity_units(5)
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap();

        // Initialize catalog
        let catalog = DynamoCatalog::new(client, &table_name);
        let client_id = String::from("ANONYMOUS");

        // Add shares
        catalog
            .put_share(client_id.clone(), ShareInfo::new("share1".to_owned(), None))
            .await
            .unwrap();
        catalog
            .put_share(client_id.clone(), ShareInfo::new("share2".to_owned(), None))
            .await
            .unwrap();

        // Add schemas
        catalog
            .put_schema(
                client_id.clone(),
                SchemaInfo::new("schema1".to_owned(), "share1".to_owned()),
            )
            .await
            .unwrap();
        catalog
            .put_schema(
                client_id.clone(),
                SchemaInfo::new("schema2".to_owned(), "share2".to_owned()),
            )
            .await
            .unwrap();
        catalog
            .put_schema(
                client_id.clone(),
                SchemaInfo::new("schema3".to_owned(), "share2".to_owned()),
            )
            .await
            .unwrap();

        // Add tables
        catalog
            .put_table(
                client_id.clone(),
                TableInfo::new(
                    "table1".to_owned(),
                    "schema1".to_owned(),
                    "share1".to_owned(),
                    "s3a://<bucket-name>/<the-table-path>".to_owned(),
                ),
            )
            .await
            .unwrap();
        catalog
            .put_table(
                client_id.clone(),
                TableInfo::new(
                    "table2".to_owned(),
                    "schema1".to_owned(),
                    "share1".to_owned(),
                    "abfss://<container-name>@<account-name}.dfs.core.windows.net/<the-table-path>"
                        .to_owned(),
                ),
            )
            .await
            .unwrap();
        catalog
            .put_table(
                client_id.clone(),
                TableInfo::new(
                    "table3".to_owned(),
                    "schema2".to_owned(),
                    "share2".to_owned(),
                    "gs://<bucket-name>/<the-table-path>".to_owned(),
                ),
            )
            .await
            .unwrap();

        let schema1 = catalog
            .get_schema("ANONYMOUS", "share1", "schema1")
            .await
            .unwrap();

        assert_eq!(schema1.share_name(), "share1");
        assert_eq!(schema1.name(), "schema1");

        catalog
    }
}
