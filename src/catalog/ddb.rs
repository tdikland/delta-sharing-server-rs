use super::{Catalog, CatalogError, Page, Pagination, SchemaInfo, ShareInfo, TableInfo};
use crate::auth::ClientId;
use async_trait::async_trait;
use aws_sdk_dynamodb::{types::AttributeValue, Client};
use base64::{engine::general_purpose, Engine as _};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Catalog implementation backed by AWS DynamoDB
pub struct DynamoCatalog {
    client: Client,
    table_name: String,
}

impl DynamoCatalog {
    pub fn new(client: Client, table_name: impl Into<String>) -> Self {
        Self {
            client,
            table_name: table_name.into(),
        }
    }

    pub async fn put_share(&self, client_id: String, share: ShareInfo) -> Result<(), CatalogError> {
        let mut builder = self
            .client
            .put_item()
            .table_name(&self.table_name)
            .item("PK", AttributeValue::S(client_id))
            .item("SK", AttributeValue::S(format!("SHARE#{}", share.name())))
            .item("name", AttributeValue::S(share.name().to_owned()));

        if let Some(share_id) = share.id() {
            builder = builder.item("id", AttributeValue::S(share_id.to_owned()));
        }

        let _result = builder
            .send()
            .await
            .map_err(|e| CatalogError::internal(e.to_string()))?;

        Ok(())
    }

    pub async fn get_share(
        &self,
        client_id: String,
        share_name: String,
    ) -> Result<ShareInfo, CatalogError> {
        let result = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key("PK", AttributeValue::S(client_id))
            .key("SK", AttributeValue::S(format!("SHARE#{}", share_name)))
            .send()
            .await
            .map_err(|e| CatalogError::internal(e.to_string()))?;

        if let Some(item) = result.item() {
            Ok(item.try_into()?)
        } else {
            Err(CatalogError::share_not_found(&share_name))
        }
    }

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
            query = query.set_exclusive_start_key(token_to_key(token));
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

    pub async fn put_schema(
        &self,
        client_id: String,
        share_name: String,
        schema: SchemaInfo,
    ) -> Result<(), CatalogError> {
        let builder = self
            .client
            .put_item()
            .table_name(&self.table_name)
            .item("PK", AttributeValue::S(client_id))
            .item(
                "SK",
                AttributeValue::S(format!("SCHEMA#{}.{}", share_name, schema.name())),
            )
            .item("share_name", AttributeValue::S(share_name))
            .item("name", AttributeValue::S(schema.name().to_owned()));

        let _result = builder
            .send()
            .await
            .map_err(|e| CatalogError::internal(e.to_string()))?;

        Ok(())
    }

    pub async fn get_schema(
        &self,
        client_id: String,
        share_name: String,
        schema_name: String,
    ) -> Result<SchemaInfo, CatalogError> {
        let result = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key("PK", AttributeValue::S(client_id))
            .key(
                "SK",
                AttributeValue::S(format!("SCHEMA#{}.{}", share_name, schema_name)),
            )
            .send()
            .await
            .map_err(|e| CatalogError::internal(e.to_string()))?;

        if let Some(item) = result.item() {
            Ok(item.try_into()?)
        } else {
            Err(CatalogError::schema_not_found(&share_name, &schema_name))
        }
    }

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
            query = query.set_exclusive_start_key(token_to_key(token));
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

    pub async fn put_table(
        &self,
        client_id: String,
        share_name: String,
        schema_name: String,
        table: TableInfo,
    ) -> Result<(), CatalogError> {
        let builder = self
            .client
            .put_item()
            .table_name(&self.table_name)
            .item("PK", AttributeValue::S(client_id))
            .item(
                "SK",
                AttributeValue::S(format!(
                    "TABLE#{}.{}.{}",
                    share_name,
                    schema_name,
                    table.name()
                )),
            )
            .item("name", AttributeValue::S(table.name().to_owned()));

        let _result = builder
            .send()
            .await
            .map_err(|e| CatalogError::internal(e.to_string()))?;

        Ok(())
    }

    pub async fn get_table(
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
            query = query.set_exclusive_start_key(token_to_key(token));
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
            query = query.set_exclusive_start_key(token_to_key(token));
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

#[derive(Serialize, Deserialize)]
struct DynamoKey {
    pk: String,
    sk: String,
}

fn token_to_key(token: &str) -> Option<HashMap<String, AttributeValue>> {
    let decoded_token = general_purpose::URL_SAFE.decode(token).unwrap();
    let key: DynamoKey = serde_json::from_slice(&decoded_token).unwrap();
    let map = HashMap::from_iter([
        (String::from("PK"), AttributeValue::S(key.pk)),
        (String::from("SK"), AttributeValue::S(key.sk)),
    ]);
    Some(map)
}

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
        let share_name = get_string_from_item(value, "name")?;
        let share_id = value.get("share_id").and_then(|v| v.as_s().ok().cloned());

        Ok(ShareInfo::new(share_name, share_id))
    }
}

impl TryFrom<&HashMap<String, AttributeValue>> for SchemaInfo {
    type Error = CatalogError;

    fn try_from(value: &HashMap<String, AttributeValue>) -> Result<Self, Self::Error> {
        let name = get_string_from_item(value, "name")?;
        let share_name = get_string_from_item(value, "share_name")?;

        Ok(Self::new(name, share_name))
    }
}

impl TryFrom<&HashMap<String, AttributeValue>> for TableInfo {
    type Error = CatalogError;

    fn try_from(value: &HashMap<String, AttributeValue>) -> Result<Self, Self::Error> {
        let name = get_string_from_item(value, "name")?;
        let schema_name = get_string_from_item(value, "schema_name")?;
        let share_name = get_string_from_item(value, "share_name")?;
        let storage_location = get_string_from_item(value, "storage_location")?;

        Ok(Self::new(name, schema_name, share_name, storage_location))
    }
}

fn get_string_from_item(
    item: &HashMap<String, AttributeValue>,
    key: &str,
) -> Result<String, CatalogError> {
    item.get(key)
        .ok_or(CatalogError::internal(format!(
            "attribute `{}` not found in item",
            key
        )))?
        .as_s()
        .map_err(|_| CatalogError::internal(format!("attribute `{key}` was not a string")))
        .cloned()
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
        self.get_share(client_id.to_string(), share_name.to_owned())
            .await
    }

    async fn get_table(
        &self,
        client_id: &ClientId,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<TableInfo, CatalogError> {
        self.get_table(
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

    #[tokio::test]
    async fn test_parse_share_info() {
        let mut item = HashMap::new();
        item.insert(
            "name".to_owned(),
            AttributeValue::S("test-share".to_owned()),
        );

        let share_info: ShareInfo = (&item).try_into().unwrap();
        assert_eq!(share_info.name(), "test-share");
    }
}
